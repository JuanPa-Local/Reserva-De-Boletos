#!/usr/bin/env bash
# iniciar_capa.sh - Inicia la capa especificada del SRB para despliegue distribuido en 3 máquinas
#
# Arquitectura:
#   M3 (Almacenamiento) : persistencia (+ Docker PostgreSQL)
#   M2 (Negocio)        : N réplicas de RMC + N réplicas de Cancelación (manifiesto en config.env)
#   M1 (Entrada/GT)     : gt (gestor de transacciones, reiniciado automáticamente si cae)
#   Clientes            : clientes (invoca lanzar_clientes.sh hacia M1)
#
# Manifiesto de réplicas (config.env):
#   REPLICAS_RMC=3  REPLICAS_CANCELACION=2  PASO_REPLICA=10
#   La réplica i escucha en PUERTO_BASE + i*PASO_REPLICA  (ej.: RMC 5556,5566,5576)
#
# Uso:
#   ./iniciar_capa.sh <persistencia|negocio|gt|clientes> [argumentos_adicionales...]
#
# Ejemplos:
#   ./iniciar_capa.sh persistencia
#   ./iniciar_capa.sh negocio [trabajadoresRMC=4] [trabajadoresCancelacion=2]
#   ./iniciar_capa.sh gt [trabajadores=4]
#   ./iniciar_capa.sh clientes --escenario 3

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/config.env" ]; then
    # shellcheck disable=SC1091
    source "$DIR/config.env"
fi

REPLICAS_RMC="${REPLICAS_RMC:-1}"
REPLICAS_CANCELACION="${REPLICAS_CANCELACION:-1}"
PASO_REPLICA="${PASO_REPLICA:-10}"

CAPA="$1"
shift || true

if [ -z "$CAPA" ]; then
    echo "================================================================================"
    echo " USO: $0 <persistencia|negocio|gt|clientes> [argumentos...]"
    echo "================================================================================"
    echo "Capas disponibles:"
    echo "  persistencia : Inicia el servicio de almacenamiento (M3: $IP_PERSISTENCIA:$PUERTO_PERSISTENCIA)"
    echo "  negocio      : Inicia $REPLICAS_RMC réplica(s) de RMC (desde $PUERTO_RMC) y $REPLICAS_CANCELACION de Cancelacion (desde $PUERTO_CANCELACION) en M2 ($IP_RMC)"
    echo "  gt           : Inicia el Gestor de Transacciones en M1 ($IP_GT:$PUERTO_GT)"
    echo "  clientes     : Lanza clientes concurrentes hacia GT ($IP_GT:$PUERTO_GT)"
    exit 1
fi

BIN_PERSISTENCIA="$DIR/capa_Servicio-Almacenamiento/persistencia"
BIN_CANCELACION="$DIR/capa_Servicios-Negocio/servicio_Cancelacion/cancelacion"
BIN_RMC="$DIR/capa_Servicios-Negocio/servicio_RMC/rmc"
BIN_GT="$DIR/capa_Servicios-Negocio/servicio_GT/gestor"
BIN_CLIENTE="$DIR/capa_Consumidores/cliente"

compilar_si_necesario() {
    local bin="$1"
    local src="$2"
    local flags="$3"
    if [ ! -f "$bin" ] || [ "$src" -nt "$bin" ]; then
        echo "[-] Compilando $(basename "$src")..."
        # shellcheck disable=SC2086
        g++ -std=c++17 -Wall -Wextra -O2 "$src" -o "$bin" $flags
        echo "[+] $(basename "$bin") compilado correctamente."
    fi
}

# Lista de endpoints "tcp://ip:puerto,..." para N réplicas con puertos base + i*paso
construir_endpoints() {
    local ip="$1" base="$2" cantidad="$3" lista="" i
    for ((i = 0; i < cantidad; i++)); do
        lista+="${lista:+,}tcp://${ip}:$((base + i * PASO_REPLICA))"
    done
    echo "$lista"
}

# Supervisor: ejecuta el comando y lo vuelve a lanzar si muere solo (proceso sustituto).
# Mantiene abierta la salida (FIFO) mientras viva, así el lector no recibe EOF entre reinicios.
# Al recibir TERM detiene al hijo, espera a que imprima su resumen y termina.
supervisar() {
    local etiqueta="$1" fifo="$2"
    shift 2
    exec > "$fifo" 2>&1
    local hijo=0
    trap 'kill -TERM "$hijo" 2>/dev/null; wait "$hijo" 2>/dev/null; exit 0' TERM
    while true; do
        "$@" &
        hijo=$!
        wait "$hijo" || true
        echo "[SUPERVISOR] $etiqueta terminó inesperadamente; reiniciando en 1 s..."
        sleep 1
    done
}

case "$CAPA" in
    persistencia)
        compilar_si_necesario "$BIN_PERSISTENCIA" "$DIR/capa_Servicio-Almacenamiento/persistencia.cpp" "-lzmq -lpqxx -lpq -lpthread"
        # Réplica de lectura de PostgreSQL (opcional): se usa si PUERTO_POSTGRES_REPLICA está definido en config.env
        CADENA_REPLICA=""
        if [ -n "${PUERTO_POSTGRES_REPLICA:-}" ]; then
            CADENA_REPLICA="host=${IP_POSTGRES_REPLICA:-127.0.0.1} port=${PUERTO_POSTGRES_REPLICA} dbname=${DB_NAME:-srb} user=${DB_USER:-postgres}"
        fi
        echo "================================================================================"
        echo " INICIANDO CAPA DE ALMACENAMIENTO (Persistencia)"
        echo " Puerto escucha : $PUERTO_PERSISTENCIA"
        echo " BD primaria    : 127.0.0.1:$PUERTO_POSTGRES ($DB_NAME)  [escrituras]"
        if [ -n "$CADENA_REPLICA" ]; then
            echo " BD réplica     : ${IP_POSTGRES_REPLICA:-127.0.0.1}:${PUERTO_POSTGRES_REPLICA}  [consultas]"
        else
            echo " BD réplica     : (sin réplica)"
        fi
        echo "================================================================================"
        export PGPASSWORD="${DB_PASSWORD:-postgres}"
        CADENA_PG="host=127.0.0.1 port=${PUERTO_POSTGRES:-5434} dbname=${DB_NAME:-srb} user=${DB_USER:-postgres}"
        detener=0
        PID_PERS=0
        trap 'detener=1; kill -TERM "$PID_PERS" 2>/dev/null || true' INT TERM
        while true; do
            "$BIN_PERSISTENCIA" "${PUERTO_PERSISTENCIA:-5558}" "$CADENA_PG" "${1:-4}" "$CADENA_REPLICA" &
            PID_PERS=$!
            wait "$PID_PERS" || true
            if [ "$detener" -eq 1 ]; then
                wait "$PID_PERS" 2>/dev/null || true
                break
            fi
            echo "[SUPERVISOR] Persistencia terminó inesperadamente; reiniciando en 1 s..."
            sleep 1
        done
        ;;

    negocio)
        compilar_si_necesario "$BIN_RMC" "$DIR/capa_Servicios-Negocio/servicio_RMC/rmc.cpp" "-lzmq -lpthread"
        compilar_si_necesario "$BIN_CANCELACION" "$DIR/capa_Servicios-Negocio/servicio_Cancelacion/servicioCancelar.cpp" "-lzmq -lpthread"
        ENDPOINT_PERSISTENCIA="tcp://${IP_PERSISTENCIA:-127.0.0.1}:${PUERTO_PERSISTENCIA:-5558}"
        TRAB_RMC="${1:-4}"
        TRAB_CANC="${2:-2}"
        echo "================================================================================"
        echo " INICIANDO CAPA DE NEGOCIO (réplicas según manifiesto en config.env)"
        echo " RMC          : $REPLICAS_RMC réplica(s) -> $(construir_endpoints "${IP_RMC:-127.0.0.1}" "${PUERTO_RMC:-5556}" "$REPLICAS_RMC")"
        echo " Cancelacion  : $REPLICAS_CANCELACION réplica(s) -> $(construir_endpoints "${IP_CANCELACION:-127.0.0.1}" "${PUERTO_CANCELACION:-5557}" "$REPLICAS_CANCELACION")"
        echo " Persistencia : $ENDPOINT_PERSISTENCIA"
        echo "================================================================================"

        FIFO_DIR="$(mktemp -d)"
        PIDS_SED=()
        PIDS_SUP=()

        # lanzar_replica <etiqueta> <fifo> <comando...>
        lanzar_replica() {
            local etiqueta="$1" fifo="$2"
            shift 2
            mkfifo "$fifo"
            # Lector con prefijo: ignora SIGINT (para no morir antes que el servicio) y termina solo en EOF
            ( trap '' INT; exec sed -u "s/^/[$etiqueta] /" < "$fifo" ) &
            PIDS_SED+=($!)
            supervisar "$etiqueta" "$fifo" "$@" &
            PIDS_SUP+=($!)
        }

        for ((i = 0; i < REPLICAS_CANCELACION; i++)); do
            lanzar_replica "CANC#$i" "$FIFO_DIR/canc$i" \
                "$BIN_CANCELACION" "$(( ${PUERTO_CANCELACION:-5557} + i * PASO_REPLICA ))" "$ENDPOINT_PERSISTENCIA" "$TRAB_CANC"
        done
        for ((i = 0; i < REPLICAS_RMC; i++)); do
            lanzar_replica "RMC#$i" "$FIFO_DIR/rmc$i" \
                "$BIN_RMC" "$(( ${PUERTO_RMC:-5556} + i * PASO_REPLICA ))" "$ENDPOINT_PERSISTENCIA" "$TRAB_RMC"
        done

        limpiado=false
        cleanup_negocio() {
            if [ "$limpiado" = true ]; then return; fi
            limpiado=true
            echo ""
            echo "[-] Deteniendo capa de negocio (${#PIDS_SUP[@]} réplicas)..."
            kill -TERM "${PIDS_SUP[@]}" 2>/dev/null || true
            wait "${PIDS_SUP[@]}" 2>/dev/null || true   # los servicios imprimen su resumen al apagarse
            wait "${PIDS_SED[@]}" 2>/dev/null || true   # los lectores vuelcan todo y terminan en EOF
            rm -rf "$FIFO_DIR"
            echo "[+] Capa de negocio detenida."
        }
        trap cleanup_negocio EXIT INT TERM

        wait "${PIDS_SUP[@]}" || true
        ;;

    gt)
        compilar_si_necesario "$BIN_GT" "$DIR/capa_Servicios-Negocio/servicio_GT/gestorTransacciones.cpp" "-lzmq -lpthread"
        ENDPOINT_RMC="$(construir_endpoints "${IP_RMC:-127.0.0.1}" "${PUERTO_RMC:-5556}" "$REPLICAS_RMC")"
        ENDPOINT_CANCELACION="$(construir_endpoints "${IP_CANCELACION:-127.0.0.1}" "${PUERTO_CANCELACION:-5557}" "$REPLICAS_CANCELACION")"
        echo "================================================================================"
        echo " INICIANDO GESTOR DE TRANSACCIONES (GT)"
        echo " Puerto clientes   : $PUERTO_GT"
        echo " Endpoints RMC     : $ENDPOINT_RMC"
        echo " Endpoints Cancelar: $ENDPOINT_CANCELACION"
        echo "================================================================================"

        detener=0
        PID_GT=0
        trap 'detener=1; kill -TERM "$PID_GT" 2>/dev/null || true' INT TERM
        while true; do
            "$BIN_GT" "${PUERTO_GT:-5555}" "$ENDPOINT_RMC" "$ENDPOINT_CANCELACION" "${1:-4}" &
            PID_GT=$!
            wait "$PID_GT" || true
            if [ "$detener" -eq 1 ]; then
                wait "$PID_GT" 2>/dev/null || true   # deja que el GT imprima su resumen
                break
            fi
            echo "[SUPERVISOR] El GT terminó inesperadamente; reiniciando en 1 s..."
            sleep 1
        done
        ;;

    clientes)
        compilar_si_necesario "$BIN_CLIENTE" "$DIR/capa_Consumidores/cliente.cpp" "-lzmq -lpthread"
        echo "================================================================================"
        echo " INICIANDO CLIENTES"
        echo " Destino GT: tcp://${IP_GT:-127.0.0.1}:${PUERTO_GT:-5555}"
        echo "================================================================================"
        if [ $# -eq 0 ]; then
            exec "$DIR/lanzar_clientes.sh" --escenario 3
        else
            exec "$DIR/lanzar_clientes.sh" "$@"
        fi
        ;;

    *)
        echo "Error: Capa desconocida '$CAPA'."
        echo "Opciones validas: persistencia | negocio | gt | clientes"
        exit 1
        ;;
esac