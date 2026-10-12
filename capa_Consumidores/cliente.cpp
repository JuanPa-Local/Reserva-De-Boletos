/*
 * cliente.cpp - Cliente del SRB (Sistema de Reserva de Boletos)
 *
 * Decisiones de diseño:
 * - Una responsabilidad por clase:
 *     LectorSolicitudes       -> lee y valida el archivo .txt de solicitudes.
 *     GestorTransacciones     -> socket REQ hacia el Gestor de Transacciones (GT),
 *                                con timeout y reconexión (patrón Lazy Pirate).
 *     ReceptorNotificaciones  -> socket PULL + hilo que recibe el aviso de
 *                                cancelación efectiva enviado por el servicio de cancelación.
 *     Estadisticas            -> promedio, desviación estándar y TPM por tipo de operación.
 * - RAII: sockets e hilo se liberan en los destructores; sin new/delete (unique_ptr).
 * - Canal según el tipo de solicitud (como pide el enunciado):
 *     CONSULTA, RESERVAR, MODIFICAR -> síncronas: el GT las enruta al RMC y responde con el resultado.
 *     CANCELAR                      -> asíncrona: el GT responde ACK de inmediato y la
 *                                      notificación llega después por el PULL del cliente.
 * - Se reintenta con el mismo id de petición, para que el servidor pueda detectar duplicados.
 *
 * Formato del archivo de solicitudes (una por línea, '#' = comentario):
 *     CONSULTA  <mes>
 *     RESERVAR  <evento> <fecha> <asientos>
 *     MODIFICAR <idReserva> <asientos> <nuevaFecha | ->
 *     CANCELAR  <idReserva>
 *   En lugar de <idReserva> se puede escribir ULTIMA (id de la última reserva exitosa).
 *
 * Compilar: g++ -std=c++17 -Wall -Wextra -O2 cliente.cpp -o cliente -lzmq -lpthread
 */

#include <zmq.hpp>

#include <atomic>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std;

constexpr int timeoutRespuestaMs = 3000;
constexpr int reintentosMaximos = 3;
constexpr int esperaNotificacionesSegundos = 5;
const string palabraUltimaReserva = "ULTIMA";

struct Configuracion {
    string idCliente;
    string endpointGestor;
    string ipCliente;
    int puertoNotificaciones = 6001;
    string archivoSolicitudes;
    int solicitudesPorMinuto = 20;
    bool modoAleatorio = false;
    int pctConsulta = 70;         // % CONSULTA
    int pctModificar = 0;         // % MODIFICAR opcional (por defecto 0)
    int duracionSegundos = 60;    // duración en segundos en modo aleatorio
    unsigned int semilla = 0;     // semilla para generador aleatorio
};

struct Solicitud {
    string tipo;
    vector<string> argumentos;
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

string unir(const vector<string>& partes, const char separador) {
    string resultado;
    for (size_t i = 0; i < partes.size(); ++i) {
        resultado += (i == 0 ? "" : string(1, separador)) + partes[i];
    }
    return resultado;
}

long long nanosAhora() {
    return chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch()).count();
}

// Cantidad de argumentos que exige cada tipo de solicitud (-1 si el tipo no existe).
int argumentosEsperados(const string& tipo) {
    if (tipo == "CONSULTA") return 1;
    if (tipo == "RESERVAR") return 3;
    if (tipo == "MODIFICAR") return 3;
    if (tipo == "CANCELAR") return 1;
    return -1;
}

// ---------------------------------------------------------------- lectura del archivo

class LectorSolicitudes {
public:
    static vector<Solicitud> leer(const string& ruta) {
        ifstream archivo(ruta);
        if (!archivo.is_open()) {
            throw runtime_error("No se pudo abrir el archivo de solicitudes: " + ruta);
        }
        vector<Solicitud> solicitudes;
        string linea;
        int numeroLinea = 0;
        while (getline(archivo, linea)) {
            ++numeroLinea;
            Solicitud solicitud;
            if (interpretarLinea(linea, solicitud)) {
                validar(solicitud, numeroLinea);
                solicitudes.push_back(solicitud);
            }
        }
        return solicitudes;
    }

private:
    // Devuelve false si la línea está vacía o es un comentario.
    static bool interpretarLinea(const string& linea, Solicitud& solicitud) {
        istringstream flujo(linea);
        flujo >> solicitud.tipo;
        if (solicitud.tipo.empty() || solicitud.tipo[0] == '#') {
            return false;
        }
        string argumento;
        while (flujo >> argumento) {
            solicitud.argumentos.push_back(argumento);
        }
        return true;
    }

    static void validar(const Solicitud& solicitud, const int numeroLinea) {
        const int esperados = argumentosEsperados(solicitud.tipo);
        if (esperados < 0) {
            throw runtime_error("Línea " + to_string(numeroLinea) + ": tipo desconocido '" + solicitud.tipo + "'");
        }
        if (static_cast<int>(solicitud.argumentos.size()) != esperados) {
            throw runtime_error("Línea " + to_string(numeroLinea) + ": " + solicitud.tipo + " requiere " +
                                to_string(esperados) + " argumento(s)");
        }
    }
};

// ---------------------------------------------------------------- comunicación con el GT

class GestorTransacciones {
public:
    GestorTransacciones(zmq::context_t& contexto, const string& endpoint, const int timeoutMs)
        : contexto_(contexto), endpoint_(endpoint), timeoutMs_(timeoutMs) {
        conectar();
    }

    string enviar(const string& mensaje) {
        for (int intento = 0; intento < reintentosMaximos; ++intento) {
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
        return "ERROR|Sin respuesta del Gestor de Transacciones";
    }

private:
    void conectar() {
        socket_ = make_unique<zmq::socket_t>(contexto_, zmq::socket_type::req);
        socket_->set(zmq::sockopt::linger, 0);
        socket_->set(zmq::sockopt::rcvtimeo, timeoutMs_);
        socket_->set(zmq::sockopt::sndtimeo, timeoutMs_);
        socket_->connect(endpoint_);
    }

    zmq::context_t& contexto_;
    const string endpoint_;
    const int timeoutMs_;
    unique_ptr<zmq::socket_t> socket_;
};

// ---------------------------------------------------------------- notificaciones de cancelación

class ReceptorNotificaciones {
public:
    ReceptorNotificaciones(zmq::context_t& contexto, const int puerto)
        : socket_(contexto, zmq::socket_type::pull) {
        socket_.set(zmq::sockopt::linger, 0);
        socket_.set(zmq::sockopt::rcvtimeo, 500);
        socket_.bind("tcp://*:" + to_string(puerto));
        hilo_ = thread(&ReceptorNotificaciones::escuchar, this);
    }

    ~ReceptorNotificaciones() {
        activo_ = false;
        if (hilo_.joinable()) {
            hilo_.join();
        }
    }

    ReceptorNotificaciones(const ReceptorNotificaciones&) = delete;
    ReceptorNotificaciones& operator=(const ReceptorNotificaciones&) = delete;

private:
    void escuchar() {
        while (activo_) {
            zmq::message_t mensaje;
            const zmq::recv_result_t recibido = socket_.recv(mensaje, zmq::recv_flags::none);
            if (recibido) {
                const string texto(static_cast<char*>(mensaje.data()), mensaje.size());
                imprimir("[NOTIFICACION] " + texto);  // NOTIF|idReserva|OK/ERROR|mensaje
            }
        }
    }

    zmq::socket_t socket_;
    atomic<bool> activo_{true};
    thread hilo_;
};

// ---------------------------------------------------------------- métricas

class Estadisticas {
public:
    void registrar(const string& tipo, const double milisegundos) {
        tiempos_[tipo].push_back(milisegundos);
    }

    void imprimirResumen(const double segundosTotales) const {
        imprimir("\n=== Resumen (" + to_string(segundosTotales) + " s) ===");
        for (const pair<const string, vector<double>>& entrada : tiempos_) {
            const vector<double>& muestras = entrada.second;
            const double cantidad = static_cast<double>(muestras.size());
            const double promedio = accumulate(muestras.begin(), muestras.end(), 0.0) / cantidad;
            double sumaCuadrados = 0.0;
            for (const double muestra : muestras) {
                sumaCuadrados += (muestra - promedio) * (muestra - promedio);
            }
            const double desviacion = sqrt(sumaCuadrados / cantidad);
            const double transaccionesPorMinuto = cantidad / (segundosTotales / 60.0);
            imprimir(entrada.first + ": n=" + to_string(muestras.size()) + "  promedio=" + to_string(promedio) +
                    " ms  desv.std=" + to_string(desviacion) + " ms  TPM=" + to_string(transaccionesPorMinuto));
        }
    }

private:
    map<string, vector<double>> tiempos_;
};

// ---------------------------------------------------------------- armado de mensajes

// Reemplaza la palabra ULTIMA por el id de la última reserva. Devuelve false si no hay ninguna.
bool resolverUltimaReserva(vector<string>& argumentos, const string& ultimaReserva) {
    for (string& argumento : argumentos) {
        if (argumento == palabraUltimaReserva) {
            if (ultimaReserva.empty()) {
                return false;
            }
            argumento = ultimaReserva;
        }
    }
    return true;
}

// Formato: TIPO|idPeticion|idCliente|argumentos...  (CANCELAR agrega el endpoint donde recibir la notificación)
string construirMensaje(const string& tipo, const string& idPeticion, const Configuracion& configuracion,
                        const vector<string>& argumentos) {
    vector<string> campos = {tipo, idPeticion, configuracion.idCliente};
    campos.insert(campos.end(), argumentos.begin(), argumentos.end());
    if (tipo == "CANCELAR") {
        campos.push_back("tcp://" + configuracion.ipCliente + ":" + to_string(configuracion.puertoNotificaciones));
    }
    return unir(campos, '|');
}

// Si una reserva fue exitosa (OK|idReserva) devuelve el id; si no, devuelve cadena vacía.
string extraerIdReserva(const string& respuesta) {
    const vector<string> campos = dividir(respuesta, '|');
    if (campos.size() >= 2 && campos[0] == "OK") {
        return campos[1];
    }
    return "";
}

// ---------------------------------------------------------------- ejecución

void ejecutarSolicitudes(const Configuracion& configuracion) {
    zmq::context_t contexto(1);
    ReceptorNotificaciones receptor(contexto, configuracion.puertoNotificaciones);
    GestorTransacciones gestor(contexto, configuracion.endpointGestor, timeoutRespuestaMs);
    Estadisticas estadisticas;

    const long long timestampCorrida =
        chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch()).count();
    const int pidCorrida = static_cast<int>(getpid());
    const string idCorrida = to_string(timestampCorrida) + "-" + to_string(pidCorrida);

    const chrono::milliseconds intervalo(60000 / configuracion.solicitudesPorMinuto);
    const chrono::steady_clock::time_point inicio = chrono::steady_clock::now();
    chrono::steady_clock::time_point proximoEnvio = inicio;
    string ultimaReserva;
    int contador = 0;

    if (configuracion.modoAleatorio) {
        // Descubrir eventos existentes en la BD mediante una consulta del mes 11
        vector<pair<string, string>> eventosDisponibles;
        const string peticionInit = configuracion.idCliente + "-" + idCorrida + "-init";
        const string msgInit = construirMensaje("CONSULTA", peticionInit, configuracion, {"11"});
        const string respInit = gestor.enviar(msgInit);
        if (respInit.rfind("OK|", 0) == 0) {
            const string datos = respInit.substr(3);
            const vector<string> items = dividir(datos, ';');
            for (const string& item : items) {
                const vector<string> campos = dividir(item, ',');
                if (campos.size() >= 2) {
                    eventosDisponibles.emplace_back(campos[0], campos[1]);
                }
            }
        }
        if (eventosDisponibles.empty()) {
            eventosDisponibles.push_back({"E1", "2026-11-01"});
            eventosDisponibles.push_back({"E2", "2026-11-01"});
        }

        mt19937 generador(configuracion.semilla != 0 ? configuracion.semilla : static_cast<unsigned int>(nanosAhora()));
        uniform_int_distribution<int> distPct(0, 99);
        uniform_int_distribution<int> distAsientos(1, 2);
        uniform_int_distribution<size_t> distEv(0, eventosDisponibles.size() - 1);

        imprimir("[" + configuracion.idCliente + "] MODO ALEATORIO: " +
                 to_string(configuracion.solicitudesPorMinuto) + " TPM, " +
                 to_string(configuracion.pctConsulta) + "% CONSULTA / " +
                 to_string(configuracion.pctModificar) + "% MODIFICAR / " +
                 to_string(100 - configuracion.pctConsulta - configuracion.pctModificar) + "% RESERVAR, duracion " +
                 to_string(configuracion.duracionSegundos) + " s (" +
                 to_string(eventosDisponibles.size()) + " eventos detectados)");

        const chrono::steady_clock::time_point finPrevisto = inicio + chrono::seconds(configuracion.duracionSegundos);
        while (chrono::steady_clock::now() < finPrevisto) {
            proximoEnvio += intervalo;
            ++contador;

            Solicitud solicitud;
            const int roll = distPct(generador);
            if (roll < configuracion.pctConsulta) {
                solicitud.tipo = "CONSULTA";
                solicitud.argumentos = {"11"};
            } else if (roll < configuracion.pctConsulta + configuracion.pctModificar) {
                if (!ultimaReserva.empty()) {
                    solicitud.tipo = "MODIFICAR";
                    solicitud.argumentos = {ultimaReserva, to_string(distAsientos(generador)), "-"};
                } else {
                    solicitud.tipo = "RESERVAR";
                    const pair<string, string>& ev = eventosDisponibles[distEv(generador)];
                    solicitud.argumentos = {ev.first, ev.second, to_string(distAsientos(generador))};
                }
            } else {
                solicitud.tipo = "RESERVAR";
                const pair<string, string>& ev = eventosDisponibles[distEv(generador)];
                solicitud.argumentos = {ev.first, ev.second, to_string(distAsientos(generador))};
            }

            const string idPeticion = configuracion.idCliente + "-" + idCorrida + "-" + to_string(contador);
            const string mensaje = construirMensaje(solicitud.tipo, idPeticion, configuracion, solicitud.argumentos);

            const chrono::steady_clock::time_point antes = chrono::steady_clock::now();
            const string respuesta = gestor.enviar(mensaje);
            const double milisegundos =
                chrono::duration<double, milli>(chrono::steady_clock::now() - antes).count();

            estadisticas.registrar(solicitud.tipo, milisegundos);
            if (solicitud.tipo == "RESERVAR" && !extraerIdReserva(respuesta).empty()) {
                ultimaReserva = extraerIdReserva(respuesta);
            }
            imprimir(mensaje + "  ->  " + respuesta + "  (" + to_string(milisegundos) + " ms)");

            if (chrono::steady_clock::now() < proximoEnvio) {
                this_thread::sleep_until(proximoEnvio);
            }
        }
    } else {
        const vector<Solicitud> solicitudes = LectorSolicitudes::leer(configuracion.archivoSolicitudes);
        imprimir("[" + configuracion.idCliente + "] MODO ARCHIVO: " + to_string(solicitudes.size()) + " solicitudes, " +
                 to_string(configuracion.solicitudesPorMinuto) + " por minuto");

        for (const Solicitud& solicitud : solicitudes) {
            proximoEnvio += intervalo;
            ++contador;

            vector<string> argumentos = solicitud.argumentos;
            if (!resolverUltimaReserva(argumentos, ultimaReserva)) {
                imprimir(solicitud.tipo + " omitida: todavía no hay una reserva exitosa (ULTIMA)");
                continue;
            }

            const string idPeticion = configuracion.idCliente + "-" + idCorrida + "-" + to_string(contador);
            const string mensaje = construirMensaje(solicitud.tipo, idPeticion, configuracion, argumentos);

            const chrono::steady_clock::time_point antes = chrono::steady_clock::now();
            const string respuesta = gestor.enviar(mensaje);
            const double milisegundos =
                chrono::duration<double, milli>(chrono::steady_clock::now() - antes).count();

            estadisticas.registrar(solicitud.tipo, milisegundos);
            if (solicitud.tipo == "RESERVAR" && !extraerIdReserva(respuesta).empty()) {
                ultimaReserva = extraerIdReserva(respuesta);
            }
            imprimir(mensaje + "  ->  " + respuesta + "  (" + to_string(milisegundos) + " ms)");

            if (chrono::steady_clock::now() < proximoEnvio) {
                this_thread::sleep_until(proximoEnvio);
            }
        }
    }

    const double segundosTotales = chrono::duration<double>(chrono::steady_clock::now() - inicio).count();
    this_thread::sleep_for(chrono::seconds(esperaNotificacionesSegundos));  // notificaciones pendientes
    estadisticas.imprimirResumen(segundosTotales);
}

int main(int argc, char** argv) {
    if (argc < 6) {
        cerr << "Uso (archivo):   " << argv[0] << " <idCliente> <endpointGT> <ipCliente> <puertoNotif> <solicitudes.txt> [solicitudesPorMinuto=20]\n"
             << "Uso (aleatorio): " << argv[0] << " <idCliente> <endpointGT> <ipCliente> <puertoNotif> aleatorio <tpm>=20 <pctConsulta>=70 <duracionSeg>=60 [semilla]\n"
             << "Flags opcionales: --archivo <f> | --aleatorio --tpm <N> --pct <P> --duracion <S> --semilla <seed>\n"
             << "Ej.: " << argv[0] << " cli1 tcp://10.0.0.2:5555 10.0.0.1 6001 solicitudes.txt 30\n";
        return 1;
    }
    try {
        Configuracion configuracion;
        configuracion.idCliente = argv[1];
        configuracion.endpointGestor = argv[2];
        configuracion.ipCliente = argv[3];
        configuracion.puertoNotificaciones = stoi(argv[4]);

        int idx = 5;
        string arg5 = argv[5];
        if (arg5 == "aleatorio" || arg5 == "--aleatorio") {
            configuracion.modoAleatorio = true;
            ++idx;
            if (idx < argc && argv[idx][0] != '-') {
                configuracion.solicitudesPorMinuto = stoi(argv[idx++]);
            }
            if (idx < argc && argv[idx][0] != '-') {
                configuracion.pctConsulta = stoi(argv[idx++]);
            }
            if (idx < argc && argv[idx][0] != '-') {
                configuracion.duracionSegundos = stoi(argv[idx++]);
            }
            if (idx < argc && argv[idx][0] != '-') {
                configuracion.semilla = static_cast<unsigned int>(stoul(argv[idx++]));
            }
            if (idx < argc && argv[idx][0] != '-') {
                configuracion.pctModificar = stoi(argv[idx++]);
            }
        } else if (arg5[0] != '-') {
            configuracion.archivoSolicitudes = arg5;
            ++idx;
            if (idx < argc && argv[idx][0] != '-') {
                configuracion.solicitudesPorMinuto = stoi(argv[idx++]);
            }
        }

        while (idx < argc) {
            string flag = argv[idx];
            if (flag == "--aleatorio") {
                configuracion.modoAleatorio = true;
                ++idx;
            } else if (flag == "--archivo" && idx + 1 < argc) {
                configuracion.archivoSolicitudes = argv[++idx];
                configuracion.modoAleatorio = false;
                ++idx;
            } else if (flag == "--tpm" && idx + 1 < argc) {
                configuracion.solicitudesPorMinuto = stoi(argv[++idx]);
                ++idx;
            } else if ((flag == "--pct" || flag == "--pct-consulta") && idx + 1 < argc) {
                configuracion.pctConsulta = stoi(argv[++idx]);
                ++idx;
            } else if ((flag == "--pct-modificar" || flag == "--pct-mod") && idx + 1 < argc) {
                configuracion.pctModificar = stoi(argv[++idx]);
                ++idx;
            } else if (flag == "--duracion" && idx + 1 < argc) {
                configuracion.duracionSegundos = stoi(argv[++idx]);
                ++idx;
            } else if (flag == "--semilla" && idx + 1 < argc) {
                configuracion.semilla = static_cast<unsigned int>(stoul(argv[++idx]));
                ++idx;
            } else {
                ++idx;
            }
        }

        if (configuracion.solicitudesPorMinuto < 20) {
            throw invalid_argument("solicitudesPorMinuto debe ser al menos 20 (actual: " +
                                   to_string(configuracion.solicitudesPorMinuto) + ")");
        }
        if (configuracion.pctConsulta < 0 || configuracion.pctConsulta > 100) {
            throw invalid_argument("pctConsulta debe estar entre 0 y 100");
        }
        if (configuracion.pctModificar < 0 || configuracion.pctModificar > 100) {
            throw invalid_argument("pctModificar debe estar entre 0 y 100");
        }
        if (configuracion.pctConsulta + configuracion.pctModificar > 100) {
            throw invalid_argument("pctConsulta + pctModificar no debe exceder 100%");
        }
        if (configuracion.modoAleatorio && configuracion.duracionSegundos <= 0) {
            throw invalid_argument("duracion debe ser mayor que 0");
        }
        if (!configuracion.modoAleatorio && configuracion.archivoSolicitudes.empty()) {
            throw invalid_argument("Debe especificar un archivo de solicitudes o usar modo aleatorio");
        }

        ejecutarSolicitudes(configuracion);
    } catch (const exception& error) {
        cerr << "Error: " << error.what() << endl;
        return 1;
    }
    return 0;
}