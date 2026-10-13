/*
 * servicioCancelar.cpp - Servicio de Cancelaciones y Notificaciones del SRB
 *
 * Decisiones de diseño:
 * - Flujo asíncrono (el cliente ya recibió su ACK del GT y no está esperando):
 *     1) PULL recibe la cancelación que el GT envió con PUSH.
 *     2) Se manda una orden de liberación de asientos al Servicio de Persistencia (REQ/REP) y se espera su respuesta.
 *     3) Con ese resultado se notifica DIRECTAMENTE al cliente (PUSH hacia el PULL que el cliente abrió; su
 *        endpoint viaja dentro del mensaje de cancelación).
 * - Un hilo principal es dueño del PULL de entrada y reparte el trabajo a hilos trabajadores por sockets inproc
 *   PUSH/PULL (los sockets de ZeroMQ no son thread-safe). Varias cancelaciones se procesan en paralelo.
 * - Cada trabajador tiene su propio REQ hacia Persistencia (timeout, reintentos y reconexión: "Lazy Pirate") y
 *   un Notificador que conserva un PUSH por cliente. Los reintentos reenvían el MISMO idPeticion para que
 *   Persistencia pueda detectar duplicados y no liberar los asientos dos veces.
 * - Si Persistencia falla o rechaza, el cliente igualmente recibe una notificación (ERROR) con el motivo.
 * - RAII: sockets e hilos se liberan en los destructores; sin new/delete (unique_ptr).
 * - Métricas: tiempo desde que llega la cancelación hasta que se notifica; resumen al terminar con Ctrl+C.
 *
 * Contratos (mensajes de texto separados por '|'):
 *   GT -> Cancelación        : CANCELAR|idPeticion|idCliente|idReserva|endpointCliente   (este servicio usa PULL + bind)
 *   Cancelación -> Persistencia: LIBERAR|idPeticion|idCliente|idReserva                  (Persistencia usa REP + bind)
 *   Persistencia -> Cancelación: OK|mensaje | RECHAZADO|motivo | ERROR|motivo
 *   Cancelación -> Cliente    : NOTIF|idReserva|OK|mensaje   o   NOTIF|idReserva|ERROR|motivo
 *
 * Compilar: g++ -std=c++17 -Wall -Wextra -O2 servicioCancelar.cpp -o cancelacion -lzmq -lpthread
 * Ejecutar: ./cancelacion <puertoEscucha> <endpointPersistencia[,endpointPersistencia2,...]> [trabajadores=2]
 *   Ej.:    ./cancelacion 5557 tcp://10.0.0.3:5558 2
 */

#include <zmq.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std;

constexpr int timeoutPersistenciaMs = 3000;
constexpr int reintentosPersistencia = 2;  // el cliente no espera: se puede insistir más que en el GT
constexpr int timeoutNotificacionMs = 2000;
constexpr int intentosNotificacion = 2;
constexpr int lingerNotificacionMs = 1000;
constexpr size_t maximoSocketsNotificacion = 64;
constexpr int timeoutLecturaMs = 200;
const string endpointTrabajos = "inproc://cancelaciones";

atomic<bool> senalRecibida{false};

void manejarSenal(int /*senal*/) {
    senalRecibida = true;
}

struct Configuracion {
    int puertoEscucha = 5557;
    vector<string> endpointsPersistencia;
    int numeroTrabajadores = 2;
};

struct SolicitudCancelacion {
    string idPeticion;
    string idCliente;
    string idReserva;
    string endpointCliente;
};

// ---------------------------------------------------------------- utilidades

void imprimir(const string& linea) {
    static mutex candado;
    const lock_guard<mutex> guardia(candado);
    cout << linea << endl;
}

vector<string> dividir(const string& texto, const char separador) {
    vector<string> partes;
    string parte;
    istringstream flujo(texto);
    while (getline(flujo, parte, separador)) {
        partes.push_back(parte);
    }
    return partes;
}

string formatear(const double valor, const int decimales) {
    ostringstream flujo;
    flujo << fixed << setprecision(decimales) << valor;
    return flujo.str();
}

long long nanosAhora() {
    return chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch()).count();
}

bool enviarMultiparte(zmq::socket_t& socket, const vector<string>& marcos) {
    for (size_t i = 0; i < marcos.size(); ++i) {
        const zmq::send_flags banderas = (i + 1 < marcos.size()) ? zmq::send_flags::sndmore : zmq::send_flags::none;
        zmq::message_t marco(marcos[i].data(), marcos[i].size());
        const zmq::send_result_t enviado = socket.send(marco, banderas);
        if (!enviado) {
            return false;
        }
    }
    return true;
}

// Devuelve un vector vacío si venció el timeout de lectura.
vector<string> recibirMultiparte(zmq::socket_t& socket) {
    vector<string> marcos;
    bool hayMasMarcos = true;
    while (hayMasMarcos) {
        zmq::message_t marco;
        const zmq::recv_result_t recibido = socket.recv(marco, zmq::recv_flags::none);
        if (!recibido) {
            return vector<string>();
        }
        marcos.emplace_back(static_cast<char*>(marco.data()), marco.size());
        hayMasMarcos = marco.more();
    }
    return marcos;
}

// CANCELAR|idPeticion|idCliente|idReserva|endpointCliente
bool interpretarSolicitud(const string& texto, SolicitudCancelacion& solicitud) {
    const vector<string> campos = dividir(texto, '|');
    if (campos.size() < 5 || campos[0] != "CANCELAR" || campos[3].empty() || campos[4].empty()) {
        return false;
    }
    solicitud.idPeticion = campos[1];
    solicitud.idCliente = campos[2];
    solicitud.idReserva = campos[3];
    solicitud.endpointCliente = campos[4];
    return true;
}

// Traduce la respuesta de Persistencia al mensaje que recibirá el cliente.
// Devuelve true en 'liberada' si Persistencia confirmó la liberación.
string construirNotificacion(const string& idReserva, const string& respuestaPersistencia, bool& liberada) {
    const vector<string> campos = dividir(respuestaPersistencia, '|');
    const string detalle = campos.size() >= 2 ? campos[1] : "";
    liberada = !campos.empty() && campos[0] == "OK";
    if (liberada) {
        return "NOTIF|" + idReserva + "|OK|" + (detalle.empty() ? "Reserva cancelada" : detalle);
    }
    return "NOTIF|" + idReserva + "|ERROR|" + (detalle.empty() ? "No se pudo cancelar la reserva" : detalle);
}

// ---------------------------------------------------------------- enlace con Persistencia

class EnlacePersistencia {
public:
    EnlacePersistencia(zmq::context_t& contexto, const vector<string>& endpoints, const int timeoutMs)
        : contexto_(contexto), endpoints_(endpoints), timeoutMs_(timeoutMs) {
        conectar();
    }

    string enviar(const string& mensaje) {
        for (int intento = 0; intento < reintentosPersistencia; ++intento) {
            try {
                zmq::message_t saliente(mensaje.data(), mensaje.size());
                const zmq::send_result_t enviado = socket_->send(saliente, zmq::send_flags::none);
                if (enviado) {
                    zmq::message_t entrante;
                    const zmq::recv_result_t recibido = socket_->recv(entrante, zmq::recv_flags::none);
                    if (recibido) {
                        return string(static_cast<char*>(entrante.data()), entrante.size());
                    }
                }
            } catch (const zmq::error_t&) {
                // se recrea el socket y se reintenta con el mismo mensaje
            }
            conectar();  // un REQ que venció el timeout queda inutilizable: se crea uno nuevo
        }
        return "ERROR|El servicio de persistencia no responde";
    }

private:
    void conectar() {
        socket_ = make_unique<zmq::socket_t>(contexto_, zmq::socket_type::req);
        socket_->set(zmq::sockopt::linger, 0);
        socket_->set(zmq::sockopt::rcvtimeo, timeoutMs_);
        socket_->set(zmq::sockopt::sndtimeo, timeoutMs_);
        socket_->set(zmq::sockopt::immediate, 1);
        for (const string& endpoint : endpoints_) {
            socket_->connect(endpoint);
        }
    }

    zmq::context_t& contexto_;
    const vector<string> endpoints_;
    const int timeoutMs_;
    unique_ptr<zmq::socket_t> socket_;
};

// ---------------------------------------------------------------- notificación al cliente

// Mantiene un PUSH por cada cliente (endpoint) para no reconectar en cada notificación.
class Notificador {
public:
    explicit Notificador(zmq::context_t& contexto) : contexto_(contexto) {}

    bool notificar(const string& endpointCliente, const string& mensaje) {
        for (int intento = 0; intento < intentosNotificacion; ++intento) {
            try {
                zmq::socket_t& socket = obtenerSocket(endpointCliente);
                zmq::message_t saliente(mensaje.data(), mensaje.size());
                const zmq::send_result_t enviado = socket.send(saliente, zmq::send_flags::none);
                if (enviado) {
                    return true;
                }
            } catch (const zmq::error_t&) {
                // se descarta el socket y se vuelve a intentar con uno nuevo
            }
            sockets_.erase(endpointCliente);
        }
        return false;
    }

private:
    zmq::socket_t& obtenerSocket(const string& endpointCliente) {
        map<string, unique_ptr<zmq::socket_t>>::iterator posicion = sockets_.find(endpointCliente);
        if (posicion == sockets_.end()) {
            if (sockets_.size() >= maximoSocketsNotificacion) {
                sockets_.clear();
            }
            unique_ptr<zmq::socket_t> socket = make_unique<zmq::socket_t>(contexto_, zmq::socket_type::push);
            socket->set(zmq::sockopt::linger, lingerNotificacionMs);  // da tiempo a entregar antes de cerrar
            socket->set(zmq::sockopt::sndtimeo, timeoutNotificacionMs);
            socket->set(zmq::sockopt::immediate, 1);  // solo enviar si la conexión con el cliente está establecida
            socket->connect(endpointCliente);
            posicion = sockets_.emplace(endpointCliente, move(socket)).first;
        }
        return *posicion->second;
    }

    zmq::context_t& contexto_;
    map<string, unique_ptr<zmq::socket_t>> sockets_;
};

// ---------------------------------------------------------------- métricas (compartidas por los trabajadores)

class Estadisticas {
public:
    void registrar(const bool liberada, const bool notificada, const double milisegundos) {
        const lock_guard<mutex> guardia(candado_);
        if (tiempos_.empty()) {
            inicio_ = chrono::steady_clock::now();
        }
        fin_ = chrono::steady_clock::now();
        tiempos_.push_back(milisegundos);
        liberadas_ += liberada ? 1 : 0;
        sinNotificar_ += notificada ? 0 : 1;
    }

    void imprimirResumen() const {
        const lock_guard<mutex> guardia(candado_);
        if (tiempos_.empty()) {
            imprimir("[CANCELACION] No se procesó ninguna cancelación.");
            return;
        }
        const double cantidad = static_cast<double>(tiempos_.size());
        const double promedio = accumulate(tiempos_.begin(), tiempos_.end(), 0.0) / cantidad;
        double sumaCuadrados = 0.0;
        for (const double muestra : tiempos_) {
            sumaCuadrados += (muestra - promedio) * (muestra - promedio);
        }
        const double desviacion = sqrt(sumaCuadrados / cantidad);
        const double segundos = max(chrono::duration<double>(fin_ - inicio_).count(), 0.001);
        imprimir("\n=== Resumen de Cancelación ===");
        imprimir("Procesadas=" + to_string(tiempos_.size()) + "  liberadas=" + to_string(liberadas_) +
                 "  rechazadas/error=" + to_string(tiempos_.size() - liberadas_) +
                 "  sin notificar=" + to_string(sinNotificar_));
        imprimir("Tiempo hasta notificar: promedio=" + formatear(promedio, 3) + " ms  desv.std=" +
                 formatear(desviacion, 3) + " ms  TPM=" + formatear(cantidad / (segundos / 60.0), 1));
    }

private:
    mutable mutex candado_;
    vector<double> tiempos_;
    chrono::steady_clock::time_point inicio_;
    chrono::steady_clock::time_point fin_;
    size_t liberadas_ = 0;
    size_t sinNotificar_ = 0;
};

// ---------------------------------------------------------------- hilo trabajador

// Toma trabajos [solicitud, tRecepcion], libera en Persistencia y notifica al cliente.
class TrabajadorCancelacion {
public:
    TrabajadorCancelacion(zmq::context_t& contexto, const vector<string>& endpointsPersistencia,
                          const atomic<bool>& activo, Estadisticas& estadisticas)
        : contexto_(contexto),
          endpointsPersistencia_(endpointsPersistencia),
          activo_(activo),
          estadisticas_(estadisticas) {
        hilo_ = thread(&TrabajadorCancelacion::ejecutar, this);
    }

    ~TrabajadorCancelacion() {
        if (hilo_.joinable()) {
            hilo_.join();
        }
    }

    TrabajadorCancelacion(const TrabajadorCancelacion&) = delete;
    TrabajadorCancelacion& operator=(const TrabajadorCancelacion&) = delete;

private:
    void ejecutar() {
        try {
            zmq::socket_t trabajos(contexto_, zmq::socket_type::pull);
            trabajos.set(zmq::sockopt::linger, 0);
            trabajos.set(zmq::sockopt::rcvtimeo, timeoutLecturaMs);
            trabajos.connect(endpointTrabajos);

            EnlacePersistencia persistencia(contexto_, endpointsPersistencia_, timeoutPersistenciaMs);
            Notificador notificador(contexto_);

            while (activo_) {
                const vector<string> trabajo = recibirMultiparte(trabajos);
                if (trabajo.size() < 2) {
                    continue;
                }
                procesar(trabajo[0], stoll(trabajo[1]), persistencia, notificador);
            }
        } catch (const exception& error) {
            imprimir(string("[CANCELACION] Trabajador terminado por error: ") + error.what());
        }
    }

    void procesar(const string& texto, const long long recepcionNs, EnlacePersistencia& persistencia,
                  Notificador& notificador) {
        SolicitudCancelacion solicitud;
        if (!interpretarSolicitud(texto, solicitud)) {
            imprimir("[CANCELACION] Solicitud inválida descartada: " + texto);
            return;
        }

        const string respuestaPersistencia =
            persistencia.enviar("LIBERAR|" + solicitud.idPeticion + "|" + solicitud.idCliente + "|" + solicitud.idReserva);

        bool liberada = false;
        const string notificacion = construirNotificacion(solicitud.idReserva, respuestaPersistencia, liberada);
        const bool notificada = notificador.notificar(solicitud.endpointCliente, notificacion);

        const double milisegundos = static_cast<double>(nanosAhora() - recepcionNs) / 1e6;
        estadisticas_.registrar(liberada, notificada, milisegundos);
        imprimir("[CANCELACION] " + texto + "\n    persistencia: " + respuestaPersistencia + "\n    notificacion a " +
                 solicitud.endpointCliente + ": " + notificacion + (notificada ? "" : "  (NO ENTREGADA)") + "  (" +
                 formatear(milisegundos, 2) + " ms)");
    }

    zmq::context_t& contexto_;
    const vector<string> endpointsPersistencia_;
    const atomic<bool>& activo_;
    Estadisticas& estadisticas_;
    thread hilo_;
};

// ---------------------------------------------------------------- servicio

class ServicioCancelacion {
public:
    ServicioCancelacion(zmq::context_t& contexto, const Configuracion& configuracion)
        : entrada_(contexto, zmq::socket_type::pull), trabajos_(contexto, zmq::socket_type::push) {
        entrada_.set(zmq::sockopt::linger, 0);
        entrada_.set(zmq::sockopt::rcvtimeo, timeoutLecturaMs);
        entrada_.bind("tcp://*:" + to_string(configuracion.puertoEscucha));

        trabajos_.set(zmq::sockopt::linger, 0);
        trabajos_.set(zmq::sockopt::sndtimeo, 1000);
        trabajos_.bind(endpointTrabajos);

        for (int i = 0; i < configuracion.numeroTrabajadores; ++i) {
            trabajadores_.push_back(make_unique<TrabajadorCancelacion>(
                contexto, configuracion.endpointsPersistencia, activo_, estadisticas_));
        }
        imprimir("[CANCELACION] Escuchando al GT en el puerto " + to_string(configuracion.puertoEscucha) + " con " +
                 to_string(configuracion.numeroTrabajadores) + " trabajador(es). Ctrl+C para terminar.");
    }

    ~ServicioCancelacion() {
        activo_ = false;  // los trabajadores (declarados al final) se detienen y se unen primero
    }

    ServicioCancelacion(const ServicioCancelacion&) = delete;
    ServicioCancelacion& operator=(const ServicioCancelacion&) = delete;

    void ejecutar() {
        while (!senalRecibida) {
            try {
                zmq::message_t mensaje;
                const zmq::recv_result_t recibido = entrada_.recv(mensaje, zmq::recv_flags::none);
                if (recibido) {
                    encolar(string(static_cast<char*>(mensaje.data()), mensaje.size()));
                }
            } catch (const zmq::error_t& error) {
                if (error.num() != EINTR) {
                    throw;
                }
            }
        }
        detener();  // termina las cancelaciones en curso antes de que main imprima el resumen
    }

    void imprimirResumen() const {
        estadisticas_.imprimirResumen();
    }

private:
    void detener() {
        activo_ = false;
        trabajadores_.clear();  // cada destructor une su hilo
    }

    void encolar(const string& solicitud) {
        const bool encolada = enviarMultiparte(trabajos_, {solicitud, to_string(nanosAhora())});
        if (!encolada) {
            imprimir("[CANCELACION] Cola llena, solicitud descartada: " + solicitud);
        }
    }

    atomic<bool> activo_{true};
    zmq::socket_t entrada_;
    zmq::socket_t trabajos_;
    Estadisticas estadisticas_;
    vector<unique_ptr<TrabajadorCancelacion>> trabajadores_;  // siempre el último miembro
};

// ---------------------------------------------------------------- main

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Uso: " << argv[0] << " <puertoEscucha> <endpointPersistencia[,endpointPersistencia2,...]> "
             << "[trabajadores=2]\n"
             << "Ej.: " << argv[0] << " 5557 tcp://127.0.0.1:5558 2\n";
        return 1;
    }
    try {
        Configuracion configuracion;
        configuracion.puertoEscucha = stoi(argv[1]);
        configuracion.endpointsPersistencia = dividir(argv[2], ',');
        if (argc >= 4) {
            configuracion.numeroTrabajadores = stoi(argv[3]);
        }
        if (configuracion.endpointsPersistencia.empty() || configuracion.numeroTrabajadores <= 0) {
            throw invalid_argument("Debe indicar al menos un endpoint de Persistencia y 1 o más trabajadores");
        }

        signal(SIGINT, manejarSenal);
        signal(SIGTERM, manejarSenal);

        zmq::context_t contexto(1);
        ServicioCancelacion servicio(contexto, configuracion);
        servicio.ejecutar();
        servicio.imprimirResumen();
    } catch (const exception& error) {
        cerr << "Error: " << error.what() << endl;
        return 1;
    }
    return 0;
}