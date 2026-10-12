/*
 * gestor_transacciones.cpp - Gestor de Transacciones (GT) del SRB
 *
 * Decisiones de diseño:
 * - El GT es la puerta de entrada: recibe a los clientes por un ROUTER (atiende muchos clientes a la vez)
 *   y enruta según el tipo de solicitud:
 *     CONSULTA, RESERVAR, MODIFICAR -> canal SÍNCRONO: se entrega a un hilo trabajador, que hace
 *                                      REQ/REP con el RMC, espera la respuesta y el GT se la devuelve al cliente.
 *     CANCELAR                      -> canal ASÍNCRONO: el GT responde ACK de inmediato (no bloquea) y
 *                                      reenvía la solicitud al servicio de cancelación con PUSH.
 * - Un hilo principal es dueño del ROUTER (los sockets de ZeroMQ no son thread-safe). Se comunica con los
 *   trabajadores por sockets inproc PUSH/PULL (trabajos y resultados), sin compartir memoria ni candados.
 * - Cada trabajador tiene su propio socket REQ hacia el RMC (con timeout y reconexión, "Lazy Pirate").
 *   REQ reparte en round-robin entre los endpoints de RMC que se le indiquen, así que añadir réplicas
 *   de RMC (entrega 2) solo exige pasar más endpoints.
 * - RAII: sockets e hilos se liberan en los destructores; sin new/delete (unique_ptr).
 * - Métricas: el GT mide el tiempo desde que recibe la solicitud hasta que responde, y al terminar
 *   (Ctrl+C) imprime promedio, desviación estándar y TPM por tipo de operación (Tabla 1 del enunciado).
 *
 * Contratos con los demás servicios (mensajes de texto separados por '|'):
 *   Cliente -> GT   : TIPO|idPeticion|idCliente|argumentos...      (el cliente usa REQ)
 *   GT -> RMC       : el mismo mensaje, intacto (el RMC usa REP o ROUTER y bind)
 *   RMC -> GT       : OK|... | RECHAZADO|motivo | ERROR|motivo     (el GT lo devuelve tal cual al cliente)
 *   GT -> Cancelación: CANCELAR|idPeticion|idCliente|idReserva|endpointCliente  (Cancelación usa PULL y bind)
 *
 * Compilar: g++ -std=c++17 -O2 gestor_transacciones.cpp -o gestor -lzmq -lpthread
 * Ejecutar: ./gestor <puertoClientes> <endpointRmc[,endpointRmc2,...]> <endpointCancelacion> [trabajadores=4]
 *   Ej.:    ./gestor 5555 tcp://127.0.0.1:5556 tcp://127.0.0.1:5557 4
 */

#include <zmq.hpp>

#include <algorithm>
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

constexpr int timeoutRmcMs = 2000;           // menor que el timeout del cliente (3000 ms)
constexpr int reintentosRmc = 1;
constexpr int intervaloPollMs = 100;
constexpr int timeoutLecturaTrabajadorMs = 200;
const string endpointTrabajos = "inproc://trabajos";
const string endpointResultados = "inproc://resultados";

atomic<bool> senalRecibida{false};

void manejarSenal(int /*senal*/) {
    senalRecibida = true;
}

struct Configuracion {
    int puertoClientes = 5555;
    vector<string> endpointsRmc;
    string endpointCancelacion;
    int numeroTrabajadores = 4;
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

string tipoDe(const string& mensaje) {
    return mensaje.substr(0, mensaje.find('|'));
}

bool esTipoSincrono(const string& tipo) {
    return tipo == "CONSULTA" || tipo == "RESERVAR" || tipo == "MODIFICAR";
}

// Envía varios marcos como un solo mensaje. Devuelve false si ZeroMQ no pudo enviarlo.
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

// Recibe un mensaje completo. Devuelve un vector vacío si venció el timeout de lectura.
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

// ---------------------------------------------------------------- enlace con el RMC

class EnlaceRmc {
public:
    EnlaceRmc(zmq::context_t& contexto, const vector<string>& endpoints, const int timeoutMs)
        : contexto_(contexto), endpoints_(endpoints), timeoutMs_(timeoutMs) {
        conectar();
    }

    string enviar(const string& mensaje) {
        for (int intento = 0; intento < reintentosRmc; ++intento) {
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
                // se recrea el socket y se reintenta
            }
            conectar();  // un REQ que venció el timeout queda inutilizable: se crea uno nuevo
        }
        return "ERROR|El servicio RMC no responde";
    }

private:
    void conectar() {
        socket_ = make_unique<zmq::socket_t>(contexto_, zmq::socket_type::req);
        socket_->set(zmq::sockopt::linger, 0);
        socket_->set(zmq::sockopt::rcvtimeo, timeoutMs_);
        socket_->set(zmq::sockopt::sndtimeo, timeoutMs_);
        socket_->set(zmq::sockopt::immediate, 1);  // solo enviar a RMC con conexión establecida
        for (const string& endpoint : endpoints_) {
            socket_->connect(endpoint);
        }
    }

    zmq::context_t& contexto_;
    const vector<string> endpoints_;
    const int timeoutMs_;
    unique_ptr<zmq::socket_t> socket_;
};

// ---------------------------------------------------------------- hilo trabajador

// Toma trabajos [identidad, solicitud, tRecepcion], consulta al RMC y devuelve
// [identidad, respuesta, solicitud, tRecepcion] al hilo principal.
class TrabajadorRmc {
public:
    TrabajadorRmc(zmq::context_t& contexto, const vector<string>& endpointsRmc, const atomic<bool>& activo)
        : contexto_(contexto), endpointsRmc_(endpointsRmc), activo_(activo) {
        hilo_ = thread(&TrabajadorRmc::ejecutar, this);
    }

    ~TrabajadorRmc() {
        if (hilo_.joinable()) {
            hilo_.join();
        }
    }

    TrabajadorRmc(const TrabajadorRmc&) = delete;
    TrabajadorRmc& operator=(const TrabajadorRmc&) = delete;

private:
    void ejecutar() {
        try {
            zmq::socket_t trabajos(contexto_, zmq::socket_type::pull);
            trabajos.set(zmq::sockopt::linger, 0);
            trabajos.set(zmq::sockopt::rcvtimeo, timeoutLecturaTrabajadorMs);
            trabajos.connect(endpointTrabajos);

            zmq::socket_t resultados(contexto_, zmq::socket_type::push);
            resultados.set(zmq::sockopt::linger, 0);
            resultados.connect(endpointResultados);

            EnlaceRmc rmc(contexto_, endpointsRmc_, timeoutRmcMs);

            while (activo_) {
                const vector<string> trabajo = recibirMultiparte(trabajos);
                if (trabajo.size() < 3) {
                    continue;
                }
                const string respuesta = rmc.enviar(trabajo[1]);
                enviarMultiparte(resultados, {trabajo[0], respuesta, trabajo[1], trabajo[2]});
            }
        } catch (const exception& error) {
            imprimir(string("[GT] Trabajador terminado por error: ") + error.what());
        }
    }

    zmq::context_t& contexto_;
    const vector<string> endpointsRmc_;
    const atomic<bool>& activo_;
    thread hilo_;
};

// ---------------------------------------------------------------- métricas

class Estadisticas {
public:
    void marcarSolicitud() {
        if (!hayInicio_) {
            inicio_ = chrono::steady_clock::now();
            fin_ = inicio_;
            hayInicio_ = true;
        }
    }

    void registrar(const string& tipo, const double milisegundos) {
        tiempos_[tipo].push_back(milisegundos);
        fin_ = chrono::steady_clock::now();
    }

    void imprimirResumen() const {
        if (tiempos_.empty()) {
            imprimir("[GT] No se procesó ninguna solicitud.");
            return;
        }
        const double segundos = max(chrono::duration<double>(fin_ - inicio_).count(), 0.001);
        imprimir("\n=== Resumen del GT (" + formatear(segundos, 2) + " s) ===");
        for (const pair<const string, vector<double>>& entrada : tiempos_) {
            const vector<double>& muestras = entrada.second;
            const double cantidad = static_cast<double>(muestras.size());
            const double promedio = accumulate(muestras.begin(), muestras.end(), 0.0) / cantidad;
            double sumaCuadrados = 0.0;
            for (const double muestra : muestras) {
                sumaCuadrados += (muestra - promedio) * (muestra - promedio);
            }
            const double desviacion = sqrt(sumaCuadrados / cantidad);
            const double transaccionesPorMinuto = cantidad / (segundos / 60.0);
            imprimir(entrada.first + ": n=" + to_string(muestras.size()) + "  promedio=" + formatear(promedio, 3) +
                     " ms  desv.std=" + formatear(desviacion, 3) + " ms  TPM=" + formatear(transaccionesPorMinuto, 1));
        }
    }

private:
    map<string, vector<double>> tiempos_;
    chrono::steady_clock::time_point inicio_;
    chrono::steady_clock::time_point fin_;
    bool hayInicio_ = false;
};

// ---------------------------------------------------------------- gestor de transacciones

class GestorTransacciones {
public:
    GestorTransacciones(zmq::context_t& contexto, const Configuracion& configuracion)
        : frontend_(contexto, zmq::socket_type::router),
          trabajos_(contexto, zmq::socket_type::push),
          resultados_(contexto, zmq::socket_type::pull),
          cancelacion_(contexto, zmq::socket_type::push) {
        frontend_.set(zmq::sockopt::linger, 0);
        frontend_.bind("tcp://*:" + to_string(configuracion.puertoClientes));

        trabajos_.set(zmq::sockopt::linger, 0);
        trabajos_.set(zmq::sockopt::sndtimeo, 1000);
        trabajos_.bind(endpointTrabajos);

        resultados_.set(zmq::sockopt::linger, 0);
        resultados_.bind(endpointResultados);

        cancelacion_.set(zmq::sockopt::linger, 0);
        cancelacion_.set(zmq::sockopt::immediate, 1);  // si Cancelación no está conectada, se informa al cliente
        cancelacion_.connect(configuracion.endpointCancelacion);

        for (int i = 0; i < configuracion.numeroTrabajadores; ++i) {
            trabajadores_.push_back(make_unique<TrabajadorRmc>(contexto, configuracion.endpointsRmc, activo_));
        }
        imprimir("[GT] Escuchando clientes en el puerto " + to_string(configuracion.puertoClientes) + " con " +
                 to_string(configuracion.numeroTrabajadores) + " trabajador(es). Ctrl+C para terminar.");
    }

    ~GestorTransacciones() {
        activo_ = false;  // los trabajadores (declarados al final) se detienen y se unen primero
    }

    GestorTransacciones(const GestorTransacciones&) = delete;
    GestorTransacciones& operator=(const GestorTransacciones&) = delete;

    void ejecutar() {
        zmq::pollitem_t elementos[] = {{static_cast<void*>(frontend_), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(resultados_), 0, ZMQ_POLLIN, 0}};
        while (!senalRecibida) {
            try {
                zmq::poll(elementos, 2, chrono::milliseconds(intervaloPollMs));
            } catch (const zmq::error_t& error) {
                if (error.num() == EINTR) {
                    continue;
                }
                throw;
            }
            if (elementos[0].revents & ZMQ_POLLIN) {
                atenderSolicitud();
            }
            if (elementos[1].revents & ZMQ_POLLIN) {
                atenderResultado();
            }
        }
    }

    void imprimirResumen() const {
        estadisticas_.imprimirResumen();
    }

private:
    // Mensaje de un cliente REQ al ROUTER: [identidad, vacío, solicitud]
    void atenderSolicitud() {
        const long long recepcionNs = nanosAhora();
        const vector<string> marcos = recibirMultiparte(frontend_);
        if (marcos.size() < 3) {
            return;
        }
        const string& identidad = marcos[0];
        const string& solicitud = marcos[2];
        const string tipo = tipoDe(solicitud);
        estadisticas_.marcarSolicitud();

        if (tipo == "CANCELAR") {
            atenderCancelacion(identidad, solicitud, recepcionNs);
        } else if (esTipoSincrono(tipo)) {
            const bool encolada = enviarMultiparte(trabajos_, {identidad, solicitud, to_string(recepcionNs)});
            if (!encolada) {
                responderCliente(identidad, "ERROR|Gestor de transacciones saturado");
            }
        } else {
            responderCliente(identidad, "ERROR|Operacion desconocida: " + tipo);
            imprimir("[GT] Solicitud inválida descartada: " + solicitud);
        }
    }

    // Canal asíncrono: ACK inmediato al cliente y reenvío no bloqueante a Cancelación.
    void atenderCancelacion(const string& identidad, const string& solicitud, const long long recepcionNs) {
        zmq::message_t mensaje(solicitud.data(), solicitud.size());
        const zmq::send_result_t reenviada = cancelacion_.send(mensaje, zmq::send_flags::dontwait);
        const string respuesta =
            reenviada ? "ACK|Cancelacion recibida" : "ERROR|Servicio de cancelacion no disponible";
        responderCliente(identidad, respuesta);
        registrarOperacion(solicitud, respuesta, recepcionNs);
    }

    // Resultado de un trabajador: [identidad, respuesta, solicitud, tRecepcion]
    void atenderResultado() {
        const vector<string> marcos = recibirMultiparte(resultados_);
        if (marcos.size() < 4) {
            return;
        }
        responderCliente(marcos[0], marcos[1]);
        registrarOperacion(marcos[2], marcos[1], stoll(marcos[3]));
    }

    void responderCliente(const string& identidad, const string& respuesta) {
        enviarMultiparte(frontend_, {identidad, "", respuesta});
    }

    void registrarOperacion(const string& solicitud, const string& respuesta, const long long recepcionNs) {
        const double milisegundos = static_cast<double>(nanosAhora() - recepcionNs) / 1e6;
        estadisticas_.registrar(tipoDe(solicitud), milisegundos);
        imprimir("[GT] " + solicitud + "  ->  " + respuesta + "  (" + formatear(milisegundos, 2) + " ms)");
    }

    atomic<bool> activo_{true};
    zmq::socket_t frontend_;
    zmq::socket_t trabajos_;
    zmq::socket_t resultados_;
    zmq::socket_t cancelacion_;
    Estadisticas estadisticas_;
    vector<unique_ptr<TrabajadorRmc>> trabajadores_;  // siempre el último miembro
};

// ---------------------------------------------------------------- main

int main(int argc, char** argv) {
    if (argc < 4) {
        cerr << "Uso: " << argv[0] << " <puertoClientes> <endpointRmc[,endpointRmc2,...]> <endpointCancelacion> "
             << "[trabajadores=4]\n"
             << "Ej.: " << argv[0] << " 5555 tcp://127.0.0.1:5556 tcp://127.0.0.1:5557 4\n";
        return 1;
    }
    try {
        Configuracion configuracion;
        configuracion.puertoClientes = stoi(argv[1]);
        configuracion.endpointsRmc = dividir(argv[2], ',');
        configuracion.endpointCancelacion = argv[3];
        if (argc >= 5) {
            configuracion.numeroTrabajadores = stoi(argv[4]);
        }
        if (configuracion.endpointsRmc.empty() || configuracion.numeroTrabajadores <= 0) {
            throw invalid_argument("Debe indicar al menos un endpoint de RMC y 1 o más trabajadores");
        }

        signal(SIGINT, manejarSenal);
        signal(SIGTERM, manejarSenal);

        zmq::context_t contexto(1);
        GestorTransacciones gestor(contexto, configuracion);
        gestor.ejecutar();
        gestor.imprimirResumen();
    } catch (const exception& error) {
        cerr << "Error: " << error.what() << endl;
        return 1;
    }
    return 0;
}