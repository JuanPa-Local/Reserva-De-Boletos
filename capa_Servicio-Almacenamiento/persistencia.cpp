/*
 * persistencia.cpp - Servicio de Persistencia del SRB (PostgreSQL + ZeroMQ)
 *
 * Decisiones de diseño:
 * - Es el único servicio que habla con la base de datos. Ofrece operaciones de alto nivel y ATÓMICAS, de modo
 *   que el RMC y el servicio de Cancelación solo reenvían mensajes y nunca coordinan transacciones entre sí.
 * - Entrada: un ROUTER (atiende varios solicitantes a la vez) + hilos trabajadores conectados por sockets
 *   inproc PUSH/PULL (los sockets de ZeroMQ no son thread-safe; el hilo principal es dueño del ROUTER).
 *   Cada trabajador tiene su propia conexión a PostgreSQL (libpqxx) y se reconecta si se cae.
 * - Concurrencia correcta (sin sobreventa): cada operación es una transacción que bloquea con
 *   SELECT ... FOR UPDATE las filas de evento que va a modificar, siempre en el mismo orden
 *   (reserva primero, luego eventos por fecha ascendente) para evitar interbloqueos. Además el esquema tiene
 *   CHECK (libres >= 0) como red de seguridad. Si PostgreSQL aborta por deadlock/serialización se reintenta.
 * - Idempotencia: un reintento con el mismo idPeticion devuelve el mismo resultado sin duplicar efectos
 *   (RESERVAR usa reserva.req_id UNIQUE; LIBERAR usa reserva.req_cancelacion; MODIFICAR es idempotente por sí misma).
 * - Al iniciar y al terminar se imprime un resumen del estado de la BD y una verificación de consistencia
 *   (asientos ocupados == suma de asientos de reservas activas), útil para la sustentación.
 * - RAII: sockets, hilos y conexiones se liberan en los destructores; sin new/delete (unique_ptr).
 *
 * Contrato (mensajes de texto separados por '|'; los solicitantes usan REQ y este servicio hace bind):
 *   CONSULTA |idPeticion|idCliente|mes                          -> OK|E1,2026-11-05,100,0;E2,... | RECHAZADO|motivo
 *   RESERVAR |idPeticion|idCliente|evento|fecha|asientos        -> OK|idReserva | RECHAZADO|motivo
 *   MODIFICAR|idPeticion|idCliente|idReserva|asientos|fecha|-   -> OK|mensaje | RECHAZADO|motivo
 *   LIBERAR  |idPeticion|idCliente|idReserva                    -> OK|mensaje | RECHAZADO|motivo
 *   Cualquier fallo interno de la BD                            -> ERROR|motivo
 *   (en MODIFICAR, la fecha '-' significa "misma fecha, solo cambia el número de asientos")
 *
 * Compilar: g++ -std=c++17 -Wall -Wextra -O2 persistencia.cpp -o persistencia -lzmq -lpqxx -lpq -lpthread
 * Ejecutar: ./persistencia <puertoEscucha> "<cadenaConexionPostgres>" [trabajadores=4]
 *   Ej.:    ./persistencia 5558 "host=127.0.0.1 port=5434 dbname=srb user=postgres password=postgres" 4
 */

#include <zmq.hpp>

#include <pqxx/pqxx>

#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdlib>
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

constexpr int intentosTransaccion = 3;
constexpr long long maximoAsientosPorSolicitud = 1000;
constexpr int intervaloPollMs = 100;
constexpr int timeoutLecturaTrabajadorMs = 200;
constexpr size_t maximoCaracteresRespuestaEnLog = 70;
const string endpointTrabajos = "inproc://trabajos-bd";
const string endpointResultados = "inproc://resultados-bd";

atomic<bool> senalRecibida{false};

void manejarSenal(int /*senal*/) {
    senalRecibida = true;
}

struct Configuracion {
    int puertoEscucha = 5558;
    string cadenaConexion;
    int numeroTrabajadores = 4;
};

struct Solicitud {
    string tipo;
    string idPeticion;
    string idCliente;
    vector<string> argumentos;
};

struct FilaReserva {
    string idCliente;
    string idEvento;
    string fecha;
    string estado;
    int asientos = 0;
};

struct Ocurrencia {
    bool existe = false;
    int libres = 0;
    int ocurrencias = 0;
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

string primeraLinea(const string& texto) {
    return texto.substr(0, texto.find('\n'));
}

// Convierte texto a entero sin lanzar excepciones. Devuelve false si no es un número completo.
bool aEntero(const string& texto, long long& valor) {
    if (texto.empty()) {
        return false;
    }
    char* fin = nullptr;
    errno = 0;
    const long long convertido = strtoll(texto.c_str(), &fin, 10);
    if (errno != 0 || fin == nullptr || *fin != '\0') {
        return false;
    }
    valor = convertido;
    return true;
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

// TIPO|idPeticion|idCliente|argumentos...
bool interpretarSolicitud(const string& texto, Solicitud& solicitud) {
    const vector<string> campos = dividir(texto, '|');
    if (campos.size() < 3) {
        return false;
    }
    solicitud.tipo = campos[0];
    solicitud.idPeticion = campos[1];
    solicitud.idCliente = campos[2];
    solicitud.argumentos.assign(campos.begin() + 3, campos.end());
    return true;
}

// Cantidad de argumentos que exige cada operación (-1 si no existe).
int argumentosEsperados(const string& tipo) {
    if (tipo == "CONSULTA") return 1;
    if (tipo == "RESERVAR") return 3;
    if (tipo == "MODIFICAR") return 3;
    if (tipo == "LIBERAR") return 1;
    return -1;
}

string textoAsientos(const int cantidad) {
    return to_string(cantidad) + (cantidad == 1 ? " asiento" : " asientos");
}

// ---------------------------------------------------------------- acceso a datos

// Una instancia por hilo trabajador: guarda su propia conexión y ejecuta una transacción por operación.
class RepositorioReservas {
public:
    explicit RepositorioReservas(const string& cadenaConexion)
        : cadenaConexion_(cadenaConexion + " connect_timeout=2") {}

    string procesar(const Solicitud& solicitud) {
        for (int intento = 1; intento <= intentosTransaccion; ++intento) {
            try {
                return despachar(solicitud);
            } catch (const pqxx::broken_connection&) {
                conexion_.reset();  // se reconecta en el próximo intento
            } catch (const pqxx::serialization_failure&) {
                // otra transacción ganó la carrera: se reintenta
            } catch (const pqxx::deadlock_detected&) {
                // PostgreSQL abortó esta transacción para romper un interbloqueo: se reintenta
            } catch (const pqxx::data_exception&) {
                return "RECHAZADO|Parametros invalidos";
            } catch (const pqxx::sql_error& error) {
                return "ERROR|Error de base de datos: " + primeraLinea(error.what());
            }
        }
        return "ERROR|Base de datos no disponible";
    }

private:
    pqxx::connection& conexion() {
        if (!conexion_) {
            conexion_ = make_unique<pqxx::connection>(cadenaConexion_);
        }
        return *conexion_;
    }

    string despachar(const Solicitud& solicitud) {
        const int esperados = argumentosEsperados(solicitud.tipo);
        if (esperados < 0) {
            return "ERROR|Operacion desconocida: " + solicitud.tipo;
        }
        if (static_cast<int>(solicitud.argumentos.size()) != esperados) {
            return "RECHAZADO|Parametros invalidos para " + solicitud.tipo;
        }
        if (solicitud.tipo == "CONSULTA") return consultar(solicitud);
        if (solicitud.tipo == "RESERVAR") return reservar(solicitud);
        if (solicitud.tipo == "MODIFICAR") return modificar(solicitud);
        return liberar(solicitud);
    }

    // ---- CONSULTA: eventos de un mes con sus asientos libres y ocupados
    string consultar(const Solicitud& solicitud) {
        long long mes = 0;
        if (!aEntero(solicitud.argumentos[0], mes) || mes < 1 || mes > 12) {
            return "RECHAZADO|Mes invalido (use 1-12)";
        }
        pqxx::read_transaction tx(conexion());
        const pqxx::result resultado = tx.exec_params(
            "SELECT id_evento, to_char(fecha, 'YYYY-MM-DD'), libres, ocupados FROM evento "
            "WHERE EXTRACT(MONTH FROM fecha) = $1::int ORDER BY length(id_evento), id_evento, fecha",
            static_cast<int>(mes));
        if (resultado.empty()) {
            return "RECHAZADO|No hay eventos en el mes " + to_string(mes);
        }
        string lista;
        for (const pqxx::row& fila : resultado) {
            lista += (lista.empty() ? "" : ";") + fila[0].as<string>() + "," + fila[1].as<string>() + "," +
                     to_string(fila[2].as<int>()) + "," + to_string(fila[3].as<int>());
        }
        return "OK|" + lista;
    }

    // ---- RESERVAR: descuenta asientos de una ocurrencia y registra la reserva
    string reservar(const Solicitud& solicitud) {
        const string& idEvento = solicitud.argumentos[0];
        const string& fecha = solicitud.argumentos[1];
        long long asientos = 0;
        if (!aEntero(solicitud.argumentos[2], asientos) || asientos <= 0 || asientos > maximoAsientosPorSolicitud) {
            return "RECHAZADO|Numero de asientos invalido";
        }
        pqxx::work tx(conexion());
        const Ocurrencia ocurrencia = bloquearOcurrencia(tx, idEvento, fecha);
        if (!ocurrencia.existe) {
            return "RECHAZADO|El evento no existe en esa fecha";
        }
        const pqxx::result previa = tx.exec_params("SELECT id_reserva FROM reserva WHERE req_id = $1", solicitud.idPeticion);
        if (!previa.empty()) {
            return "OK|" + to_string(previa[0][0].as<long long>());  // reintento de una reserva ya confirmada
        }
        if (ocurrencia.libres < asientos) {
            return "RECHAZADO|No hay suficientes asientos libres (disponibles: " + to_string(ocurrencia.libres) + ")";
        }
        tx.exec_params("UPDATE evento SET libres = libres - $3::int, ocupados = ocupados + $3::int "
                       "WHERE id_evento = $1 AND fecha = $2::date",
                       idEvento, fecha, asientos);
        const pqxx::result nueva = tx.exec_params(
            "INSERT INTO reserva (id_cliente, id_evento, fecha, asientos, req_id) "
            "VALUES ($1, $2, $3::date, $4::int, $5) RETURNING id_reserva",
            solicitud.idCliente, idEvento, fecha, asientos, solicitud.idPeticion);
        tx.commit();
        return "OK|" + to_string(nueva[0][0].as<long long>());
    }

    // ---- MODIFICAR: cambia el número de asientos y/o la fecha (otra ocurrencia del mismo evento)
    string modificar(const Solicitud& solicitud) {
        long long idReserva = 0;
        long long nuevosAsientos = 0;
        if (!aEntero(solicitud.argumentos[0], idReserva) || !aEntero(solicitud.argumentos[1], nuevosAsientos) ||
            nuevosAsientos <= 0 || nuevosAsientos > maximoAsientosPorSolicitud) {
            return "RECHAZADO|Parametros invalidos";
        }
        pqxx::work tx(conexion());
        FilaReserva reserva;
        if (!leerReservaBloqueada(tx, idReserva, reserva)) {
            return "RECHAZADO|La reserva no existe";
        }
        if (reserva.idCliente != solicitud.idCliente) {
            return "RECHAZADO|La reserva no pertenece a este cliente";
        }
        if (reserva.estado != "ACTIVA") {
            return "RECHAZADO|La reserva esta cancelada";
        }

        const bool cambiaFecha = solicitud.argumentos[2] != "-" && solicitud.argumentos[2] != reserva.fecha;
        const string fechaDestino = cambiaFecha ? solicitud.argumentos[2] : reserva.fecha;
        const int asientosNuevos = static_cast<int>(nuevosAsientos);

        // Bloqueo de eventos siempre en orden de fecha ascendente (evita interbloqueos).
        Ocurrencia origen;
        Ocurrencia destino;
        if (!cambiaFecha) {
            origen = bloquearOcurrencia(tx, reserva.idEvento, reserva.fecha);
            destino = origen;
        } else if (reserva.fecha < fechaDestino) {
            origen = bloquearOcurrencia(tx, reserva.idEvento, reserva.fecha);
            destino = bloquearOcurrencia(tx, reserva.idEvento, fechaDestino);
        } else {
            destino = bloquearOcurrencia(tx, reserva.idEvento, fechaDestino);
            origen = bloquearOcurrencia(tx, reserva.idEvento, reserva.fecha);
        }
        if (!destino.existe) {
            return origen.ocurrencias < 2
                       ? "RECHAZADO|El evento tiene una sola ocurrencia: solo admite cambiar el numero de asientos"
                       : "RECHAZADO|El evento no tiene ocurrencia en esa fecha";
        }

        if (!cambiaFecha) {
            const int diferencia = asientosNuevos - reserva.asientos;
            if (diferencia > destino.libres) {
                return "RECHAZADO|No hay asientos suficientes; la reserva queda como estaba";
            }
            if (diferencia != 0) {
                tx.exec_params("UPDATE evento SET libres = libres - $3::int, ocupados = ocupados + $3::int "
                               "WHERE id_evento = $1 AND fecha = $2::date",
                               reserva.idEvento, reserva.fecha, diferencia);
            }
        } else {
            if (asientosNuevos > destino.libres) {
                return "RECHAZADO|No hay asientos suficientes en la nueva fecha; la reserva queda como estaba";
            }
            tx.exec_params("UPDATE evento SET libres = libres + $3::int, ocupados = ocupados - $3::int "
                           "WHERE id_evento = $1 AND fecha = $2::date",
                           reserva.idEvento, reserva.fecha, reserva.asientos);
            tx.exec_params("UPDATE evento SET libres = libres - $3::int, ocupados = ocupados + $3::int "
                           "WHERE id_evento = $1 AND fecha = $2::date",
                           reserva.idEvento, fechaDestino, asientosNuevos);
        }
        tx.exec_params("UPDATE reserva SET fecha = $2::date, asientos = $3::int WHERE id_reserva = $1", idReserva,
                       fechaDestino, asientosNuevos);
        tx.commit();
        return "OK|Reserva " + to_string(idReserva) + " modificada: " + textoAsientos(asientosNuevos) + " el " + fechaDestino;
    }

    // ---- LIBERAR: cancela una reserva y devuelve sus asientos al evento
    string liberar(const Solicitud& solicitud) {
        long long idReserva = 0;
        if (!aEntero(solicitud.argumentos[0], idReserva)) {
            return "RECHAZADO|Identificador de reserva invalido";
        }
        pqxx::work tx(conexion());
        FilaReserva reserva;
        if (!leerReservaBloqueada(tx, idReserva, reserva)) {
            return "RECHAZADO|La reserva no existe";
        }
        if (reserva.idCliente != solicitud.idCliente) {
            return "RECHAZADO|La reserva no pertenece a este cliente";
        }
        const string mensajeExito = "Reserva " + to_string(idReserva) + " cancelada: " +
                                    textoAsientos(reserva.asientos) + " liberados";
        if (reserva.estado == "CANCELADA") {
            const pqxx::result mismoReintento = tx.exec_params(
                "SELECT 1 FROM reserva WHERE id_reserva = $1 AND req_cancelacion = $2", idReserva, solicitud.idPeticion);
            return mismoReintento.empty() ? "RECHAZADO|La reserva ya estaba cancelada" : "OK|" + mensajeExito;
        }
        bloquearOcurrencia(tx, reserva.idEvento, reserva.fecha);
        tx.exec_params("UPDATE evento SET libres = libres + $3::int, ocupados = ocupados - $3::int "
                       "WHERE id_evento = $1 AND fecha = $2::date",
                       reserva.idEvento, reserva.fecha, reserva.asientos);
        tx.exec_params("UPDATE reserva SET estado = 'CANCELADA', req_cancelacion = $2 WHERE id_reserva = $1",
                       idReserva, solicitud.idPeticion);
        tx.commit();
        return "OK|" + mensajeExito;
    }

    // ---- ayudas de consulta con bloqueo
    static Ocurrencia bloquearOcurrencia(pqxx::work& tx, const string& idEvento, const string& fecha) {
        const pqxx::result resultado = tx.exec_params(
            "SELECT libres, ocurrencias FROM evento WHERE id_evento = $1 AND fecha = $2::date FOR UPDATE", idEvento,
            fecha);
        Ocurrencia ocurrencia;
        if (!resultado.empty()) {
            ocurrencia.existe = true;
            ocurrencia.libres = resultado[0][0].as<int>();
            ocurrencia.ocurrencias = resultado[0][1].as<int>();
        }
        return ocurrencia;
    }

    static bool leerReservaBloqueada(pqxx::work& tx, const long long idReserva, FilaReserva& reserva) {
        const pqxx::result resultado = tx.exec_params(
            "SELECT id_cliente, id_evento, to_char(fecha, 'YYYY-MM-DD'), asientos, estado FROM reserva "
            "WHERE id_reserva = $1 FOR UPDATE",
            idReserva);
        if (resultado.empty()) {
            return false;
        }
        reserva.idCliente = resultado[0][0].as<string>();
        reserva.idEvento = resultado[0][1].as<string>();
        reserva.fecha = resultado[0][2].as<string>();
        reserva.asientos = resultado[0][3].as<int>();
        reserva.estado = resultado[0][4].as<string>();
        return true;
    }

    const string cadenaConexion_;
    unique_ptr<pqxx::connection> conexion_;
};

// ---------------------------------------------------------------- resumen del estado de la BD

class ResumenBaseDatos {
public:
    static string describir(pqxx::connection& conexion) {
        pqxx::read_transaction tx(conexion);
        const pqxx::row eventos =
            tx.exec1("SELECT count(*), COALESCE(sum(libres), 0), COALESCE(sum(ocupados), 0) FROM evento");
        const pqxx::row reservas =
            tx.exec1("SELECT count(*), COALESCE(sum(asientos), 0) FROM reserva WHERE estado = 'ACTIVA'");
        const long long ocupados = eventos[2].as<long long>();
        const long long asientosActivos = reservas[1].as<long long>();
        return "ocurrencias=" + to_string(eventos[0].as<long long>()) + "  asientos libres=" +
               to_string(eventos[1].as<long long>()) + "  ocupados=" + to_string(ocupados) +
               "  reservas activas=" + to_string(reservas[0].as<long long>()) + "  asientos en reservas activas=" +
               to_string(asientosActivos) + (ocupados == asientosActivos ? "  [CONSISTENTE]" : "  [INCONSISTENTE]");
    }
};

void imprimirEstadoBaseDatos(const string& titulo, const string& cadenaConexion) {
    pqxx::connection conexion(cadenaConexion + " connect_timeout=2");
    imprimir("[PERSISTENCIA] " + titulo + ": " + ResumenBaseDatos::describir(conexion));
}

// ---------------------------------------------------------------- hilo trabajador

// Toma trabajos [identidad, solicitud, tRecepcion], ejecuta la transacción y devuelve
// [identidad, respuesta, solicitud, tRecepcion] al hilo principal.
class TrabajadorPersistencia {
public:
    TrabajadorPersistencia(zmq::context_t& contexto, const string& cadenaConexion, const atomic<bool>& activo)
        : contexto_(contexto), cadenaConexion_(cadenaConexion), activo_(activo) {
        hilo_ = thread(&TrabajadorPersistencia::ejecutar, this);
    }

    ~TrabajadorPersistencia() {
        if (hilo_.joinable()) {
            hilo_.join();
        }
    }

    TrabajadorPersistencia(const TrabajadorPersistencia&) = delete;
    TrabajadorPersistencia& operator=(const TrabajadorPersistencia&) = delete;

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

            RepositorioReservas repositorio(cadenaConexion_);
            while (activo_) {
                const vector<string> trabajo = recibirMultiparte(trabajos);
                if (trabajo.size() < 3) {
                    continue;
                }
                enviarMultiparte(resultados, {trabajo[0], responder(repositorio, trabajo[1]), trabajo[1], trabajo[2]});
            }
        } catch (const exception& error) {
            imprimir(string("[PERSISTENCIA] Trabajador terminado por error: ") + error.what());
        }
    }

    static string responder(RepositorioReservas& repositorio, const string& texto) {
        Solicitud solicitud;
        if (!interpretarSolicitud(texto, solicitud)) {
            return "ERROR|Mensaje mal formado";
        }
        try {
            return repositorio.procesar(solicitud);
        } catch (const exception& error) {
            return string("ERROR|") + primeraLinea(error.what());  // p. ej. no se pudo abrir la conexión
        }
    }

    zmq::context_t& contexto_;
    const string cadenaConexion_;
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
            imprimir("[PERSISTENCIA] No se procesó ninguna solicitud.");
            return;
        }
        const double segundos = max(chrono::duration<double>(fin_ - inicio_).count(), 0.001);
        imprimir("\n=== Resumen de Persistencia (" + formatear(segundos, 2) + " s) ===");
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

class ServicioPersistencia {
public:
    ServicioPersistencia(zmq::context_t& contexto, const Configuracion& configuracion)
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
                make_unique<TrabajadorPersistencia>(contexto, configuracion.cadenaConexion, activo_));
        }
        imprimir("[PERSISTENCIA] Escuchando en el puerto " + to_string(configuracion.puertoEscucha) + " con " +
                 to_string(configuracion.numeroTrabajadores) + " trabajador(es). Ctrl+C para terminar.");
    }

    ~ServicioPersistencia() {
        activo_ = false;
    }

    ServicioPersistencia(const ServicioPersistencia&) = delete;
    ServicioPersistencia& operator=(const ServicioPersistencia&) = delete;

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
        trabajadores_.clear();  // cada destructor une su hilo
    }

    // Mensaje de un solicitante REQ al ROUTER: [identidad, vacío, solicitud]
    void atenderSolicitud() {
        const long long recepcionNs = nanosAhora();
        const vector<string> marcos = recibirMultiparte(frontend_);
        if (marcos.size() < 3) {
            return;
        }
        estadisticas_.marcarSolicitud();
        const bool encolada = enviarMultiparte(trabajos_, {marcos[0], marcos[2], to_string(recepcionNs)});
        if (!encolada) {
            enviarMultiparte(frontend_, {marcos[0], "", "ERROR|Persistencia saturada"});
        }
    }

    // Resultado de un trabajador: [identidad, respuesta, solicitud, tRecepcion]
    void atenderResultado() {
        const vector<string> marcos = recibirMultiparte(resultados_);
        if (marcos.size() < 4) {
            return;
        }
        enviarMultiparte(frontend_, {marcos[0], "", marcos[1]});
        const double milisegundos = static_cast<double>(nanosAhora() - stoll(marcos[3])) / 1e6;
        estadisticas_.registrar(tipoDe(marcos[2]), milisegundos);
        imprimir("[PERSISTENCIA] " + marcos[2] + "  ->  " + recortar(marcos[1], maximoCaracteresRespuestaEnLog) + "  (" +
                 formatear(milisegundos, 2) + " ms)");
    }

    atomic<bool> activo_{true};
    zmq::socket_t frontend_;
    zmq::socket_t trabajos_;
    zmq::socket_t resultados_;
    Estadisticas estadisticas_;
    vector<unique_ptr<TrabajadorPersistencia>> trabajadores_;  // siempre el último miembro
};

// ---------------------------------------------------------------- main

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Uso: " << argv[0] << " <puertoEscucha> \"<cadenaConexionPostgres>\" [trabajadores=4]\n"
             << "Ej.: " << argv[0]
             << " 5558 \"host=127.0.0.1 port=5434 dbname=srb user=postgres password=postgres\" 4\n";
        return 1;
    }
    try {
        Configuracion configuracion;
        configuracion.puertoEscucha = stoi(argv[1]);
        configuracion.cadenaConexion = argv[2];
        if (argc >= 4) {
            configuracion.numeroTrabajadores = stoi(argv[3]);
        }
        if (configuracion.numeroTrabajadores <= 0) {
            throw invalid_argument("El numero de trabajadores debe ser mayor que 0");
        }

        signal(SIGINT, manejarSenal);
        signal(SIGTERM, manejarSenal);

        imprimirEstadoBaseDatos("Estado inicial de la BD", configuracion.cadenaConexion);

        zmq::context_t contexto(1);
        ServicioPersistencia servicio(contexto, configuracion);
        servicio.ejecutar();
        servicio.imprimirResumen();

        imprimirEstadoBaseDatos("Estado final de la BD", configuracion.cadenaConexion);
    } catch (const exception& error) {
        cerr << "Error: " << error.what() << endl;
        return 1;
    }
    return 0;
}