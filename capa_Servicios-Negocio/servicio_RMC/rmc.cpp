/*
 * rmc.cpp - Servicio de Reserva, Modificación y Consulta (RMC) del SRB
 *
 * Decisiones de diseño:
 * - Es el componente intermedio del canal síncrono del SRB (entre el Gestor de Transacciones y Persistencia).
 * - NO accede directamente a la base de datos (Persistencia es el único servicio que usa libpqxx).
 *   El RMC solo enruta solicitudes y respuestas mediante ZeroMQ.
 * - Entrada: socket ROUTER (hace bind en el puerto configurado y atiende al GT, que se conecta con REQ).
 * - Concurrencia segura: un hilo principal es dueño del ROUTER de entrada y se comunica con hilos trabajadores
 *   mediante sockets inproc PUSH/PULL (los sockets de ZeroMQ no son thread-safe).
 * - Cada trabajador tiene su propio socket REQ hacia Persistencia con patrón Lazy Pirate (timeout, reintentos
 *   con el mismo idPeticion para preservar idempotencia y reconexión ante fallos).
 * - Espera la respuesta de Persistencia y la devuelve tal cual al GT.
 * - Logs por cada operación con su resultado y tiempo de respuesta.
 * - Métricas: promedio, desviación estándar y transacciones por minuto (TPM) usando <chrono>,
 *   presentadas en un resumen al finalizar con Ctrl+C (SIGINT/SIGTERM).
 * - RAII: sockets e hilos se liberan en destructores; sin new/delete (unique_ptr).
 *
 * Contratos:
 *   GT -> RMC          : TIPO|idPeticion|idCliente|argumentos...  (GT usa REQ; RMC hace bind)
 *   RMC -> Persistencia: TIPO|idPeticion|idCliente|argumentos...  (RMC usa REQ hacia Persistencia)
 *   Persistencia -> RMC: OK|... | RECHAZADO|motivo | ERROR|motivo
 *   RMC -> GT          : devuelve la respuesta de Persistencia tal cual
 *
 * Compilar: g++ -std=c++17 -O2 rmc.cpp -o rmc -lzmq -lpthread
 * Ejecutar: ./rmc <puertoEscucha> <endpointPersistencia[,endpointPersistencia2,...]> [trabajadores=4]
 *   Ej.:    ./rmc 5556 tcp://127.0.0.1:5558 4
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

constexpr int timeoutPersistenciaMs = 2500;
constexpr int reintentosPersistencia = 2;
constexpr int intervaloPollMs = 100;
constexpr int timeoutLecturaTrabajadorMs = 200;
constexpr size_t maximoCaracteresRespuestaEnLog = 70;
const string endpointTrabajos = "inproc://rmc-trabajos";
const string endpointResultados = "inproc://rmc-resultados";

atomic<bool> senalRecibida{false};

void manejarSenal(int /*senal*/) {
    senalRecibida = true;
}

struct Configuracion {
    int puertoEscucha = 5556;
    vector<string> endpointsPersistencia;
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

string recortar(const string& texto, const size_t maximo) {
    return texto.size() <= maximo ? texto : texto.substr(0, maximo) + "...";
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
                // se recrea el socket y se reintenta
            }
            conectar();  // patrón Lazy Pirate: recrear socket REQ tras timeout
        }
        return "ERROR|El servicio de Persistencia no responde";
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

// ---------------------------------------------------------------- hilos trabajadores

// Toma trabajos [identidad, solicitud, tRecepcion], reenvía a Persistencia y devuelve
// [identidad, respuesta, solicitud, tRecepcion] al hilo principal.
class TrabajadorRmc {
public:
    TrabajadorRmc(zmq::context_t& contexto, const vector<string>& endpointsPersistencia, const atomic<bool>& activo)
        : contexto_(contexto), endpointsPersistencia_(endpointsPersistencia), activo_(activo) {
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

            EnlacePersistencia persistencia(contexto_, endpointsPersistencia_, timeoutPersistenciaMs);

            while (activo_) {
                const vector<string> trabajo = recibirMultiparte(trabajos);
                if (trabajo.size() < 3) {
                    continue;
                }
                const string respuesta = persistencia.enviar(trabajo[1]);
                enviarMultiparte(resultados, {trabajo[0], respuesta, trabajo[1], trabajo[2]});
            }
        } catch (const exception& error) {
            imprimir(string("[RMC] Trabajador terminado por error: ") + error.what());
        }
    }

    zmq::context_t& contexto_;
    const vector<string> endpointsPersistencia_;
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
            imprimir("[RMC] No se procesó ninguna solicitud.");
            return;
        }
        const double segundos = max(chrono::duration<double>(fin_ - inicio_).count(), 0.001);
        imprimir("\n=== Resumen de RMC (" + formatear(segundos, 2) + " s) ===");
        for (const pair<const string, vector<double>>& entrada : tiempos_) {
            const vector<double>& muestras = entrada.second;
            const double cantidad = static_cast<double>(muestras.size());
            const double promedio = accumulate(muestras.begin(), muestras.end(), 0.0) / cantidad;
            double sumaCuadrados = 0.0;
            for (const double muestra : muestras) {
                sumaCuadrados += (muestra - promedio) * (muestra - promedio);
            }
            const double desviacion = sqrt(sumaCuadrados / cantidad);
            imprimir(entrada.first + ": n=" + to_string(muestras.size()) + "  promedio=" + formatear(promedio, 3) +
                     " ms  desv.std=" + formatear(desviacion, 3) + " ms  TPM=" +
                     formatear(cantidad / (segundos / 60.0), 1));
        }
    }

private:
    map<string, vector<double>> tiempos_;
    chrono::steady_clock::time_point inicio_;
    chrono::steady_clock::time_point fin_;
    bool hayInicio_ = false;
};

// ---------------------------------------------------------------- servicio

class ServicioRmc {
public:
    ServicioRmc(zmq::context_t& contexto, const Configuracion& configuracion)
        : frontend_(contexto, zmq::socket_type::router),
          trabajos_(contexto, zmq::socket_type::push),
          resultados_(contexto, zmq::socket_type::pull) {
        frontend_.set(zmq::sockopt::linger, 0);
        frontend_.bind("tcp://*:" + to_string(configuracion.puertoEscucha));

        trabajos_.set(zmq::sockopt::linger, 0);
        trabajos_.set(zmq::sockopt::sndtimeo, 1000);
        trabajos_.bind(endpointTrabajos);

        resultados_.set(zmq::sockopt::linger, 0);
        resultados_.bind(endpointResultados);

        for (int i = 0; i < configuracion.numeroTrabajadores; ++i) {
            trabajadores_.push_back(
                make_unique<TrabajadorRmc>(contexto, configuracion.endpointsPersistencia, activo_));
        }
        imprimir("[RMC] [INFO] Escuchando en el puerto " + to_string(configuracion.puertoEscucha) + " con " +
                 to_string(configuracion.numeroTrabajadores) + " trabajador(es). Ctrl+C para terminar.");
    }

    ~ServicioRmc() {
        activo_ = false;
    }

    ServicioRmc(const ServicioRmc&) = delete;
    ServicioRmc& operator=(const ServicioRmc&) = delete;

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
        detener();
    }

    void imprimirResumen() const {
        estadisticas_.imprimirResumen();
    }

private:
    void detener() {
        activo_ = false;
        trabajadores_.clear();
    }

    // Solicitud del GT: [identidad, vacío, solicitud]
    void atenderSolicitud() {
        const long long recepcionNs = nanosAhora();
        const vector<string> marcos = recibirMultiparte(frontend_);
        if (marcos.size() < 3) {
            return;
        }
        estadisticas_.marcarSolicitud();
        const bool encolada = enviarMultiparte(trabajos_, {marcos[0], marcos[2], to_string(recepcionNs)});
        if (!encolada) {
            enviarMultiparte(frontend_, {marcos[0], "", "ERROR|RMC saturado"});
        }
    }

    // Respuesta de un trabajador: [identidad, respuesta, solicitud, tRecepcion]
    void atenderResultado() {
        const vector<string> marcos = recibirMultiparte(resultados_);
        if (marcos.size() < 4) {
            return;
        }
        enviarMultiparte(frontend_, {marcos[0], "", marcos[1]});
        const double milisegundos = static_cast<double>(nanosAhora() - stoll(marcos[3])) / 1e6;
        estadisticas_.registrar(tipoDe(marcos[2]), milisegundos);
        string etiqueta = "[INFO]";
        if (marcos[1].rfind("OK|", 0) == 0) {
            etiqueta = "[EXITO]";
        } else if (marcos[1].rfind("RECHAZADO", 0) == 0) {
            etiqueta = "[RECHAZO]";
        } else if (marcos[1].rfind("ERROR|", 0) == 0) {
            etiqueta = "[ERROR]";
        }
        imprimir("[RMC] " + etiqueta + " " + marcos[2] + "  ->  " + recortar(marcos[1], maximoCaracteresRespuestaEnLog) + "  (" +
                 formatear(milisegundos, 2) + " ms)");
    }

    atomic<bool> activo_{true};
    zmq::socket_t frontend_;
    zmq::socket_t trabajos_;
    zmq::socket_t resultados_;
    Estadisticas estadisticas_;
    vector<unique_ptr<TrabajadorRmc>> trabajadores_;
};

// ---------------------------------------------------------------- main

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Uso: " << argv[0] << " <puertoEscucha> <endpointPersistencia[,endpointPersistencia2,...]> [trabajadores=4]\n"
             << "Ej.: " << argv[0] << " 5556 tcp://127.0.0.1:5558 4\n";
        return 1;
    }
    try {
        Configuracion configuracion;
        configuracion.puertoEscucha = stoi(argv[1]);
        configuracion.endpointsPersistencia = dividir(argv[2], ',');
        if (configuracion.endpointsPersistencia.empty()) {
            throw invalid_argument("Debe especificar al menos un endpoint de Persistencia");
        }
        if (argc >= 4) {
            configuracion.numeroTrabajadores = stoi(argv[3]);
        }
        if (configuracion.numeroTrabajadores <= 0) {
            throw invalid_argument("El numero de trabajadores debe ser mayor que 0");
        }

        signal(SIGINT, manejarSenal);
        signal(SIGTERM, manejarSenal);

        zmq::context_t contexto(1);
        ServicioRmc servicio(contexto, configuracion);
        servicio.ejecutar();
        servicio.imprimirResumen();
    } catch (const exception& error) {
        cerr << "Error: " << error.what() << endl;
        return 1;
    }
    return 0;
}
