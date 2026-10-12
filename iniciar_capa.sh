#!/usr/bin/env bash
# iniciar_capa.sh - Inicia la capa especificada del SRB para despliegue distribuido en 3 máquinas
#
# Arquitectura:
#   M3 (Almacenamiento) : persistencia (+ Docker PostgreSQL)
#   M2 (Negocio)        : negocio (rmc + cancelacion en paralelo con logs visibles)
#   M1 (Entrada/GT)     : gt (gestor de transacciones)
#   Clientes            : clientes (invoca lanzar_clientes.sh hacia M1)
#
# Uso:
#   ./iniciar_capa.sh <persistencia|negocio|gt|clientes> [argumentos_adicionales...]
#
# Ejemplos:
#   ./iniciar_capa.sh persistencia
#   ./iniciar_capa.sh negocio
#   ./iniciar_capa.sh gt
#   ./iniciar_capa.sh clientes --escenario 3
#   ./iniciar_capa.sh clientes --escenario 5 30 20
#   ./iniciar_capa.sh clientes 5 aleatorio 30 70 20

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/config.env" ]; then
    # shellcheck disable=SC1091
    source "$DIR/config.env"
fi

CAPA="$1"
shift || true

if [ -z "$CAPA" ]; then
    echo "================================================================================"
    echo " USO: $0 <persistencia|negocio|gt|clientes> [argumentos...]"
    echo "================================================================================"
    echo "Capas disponibles:"
    echo "  persistencia : Inicia el servicio de almacenamiento (M3: $IP_PERSISTENCIA:$PUERTO_PERSISTENCIA)"
    echo "  negocio      : Inicia RMC ($PUERTO_RMC) y Cancelacion ($PUERTO_CANCELACION) en M2 ($IP_RMC)"
    echo "  gt           : Inicia el Gestor de Transacciones en M1 ($IP_GT:$PUERTO_GT)"
    echo "  clientes     : Lanza clientes concurrentes hacia GT ($IP_GT:$PUERTO_GT)"
    echo ""
    echo "Ejemplos:"
    echo "  $0 persistencia"
    echo "  $0 negocio"
    echo "  $0 gt"
    echo "  $0 clientes --escenario 3"
    echo "  $0 clientes --escenario 5 30 20"
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

case "$CAPA" in
    persistencia)
        compilar_si_necesario "$BIN_PERSISTENCIA" "$DIR/capa_Servicio-Almacenamiento/persistencia.cpp" "-lzmq -lpqxx -lpq -lpthread"
        echo "================================================================================"
        echo " INICIANDO CAPA DE ALMACENAMIENTO (Persistencia)"
        echo " Puerto escucha: $PUERTO_PERSISTENCIA"
        echo " Base de datos : 127.0.0.1:$PUERTO_POSTGRES ($DB_NAME)"
        echo "================================================================================"
        export PGPASSWORD="${DB_PASSWORD:-postgres}"
        CADENA_PG="host=127.0.0.1 port=${PUERTO_POSTGRES:-5434} dbname=${DB_NAME:-srb} user=${DB_USER:-postgres}"
        exec "$BIN_PERSISTENCIA" "${PUERTO_PERSISTENCIA:-5558}" "$CADENA_PG" "${1:-4}"
        ;;

    negocio)
        compilar_si_necesario "$BIN_RMC" "$DIR/capa_Servicios-Negocio/servicio_RMC/rmc.cpp" "-lzmq -lpthread"
        compilar_si_necesario "$BIN_CANCELACION" "$DIR/capa_Servicios-Negocio/servicio_Cancelacion/servicioCancelar.cpp" "-lzmq -lpthread"
        ENDPOINT_PERSISTENCIA="tcp://${IP_PERSISTENCIA:-127.0.0.1}:${PUERTO_PERSISTENCIA:-5558}"
        echo "================================================================================"
        echo " INICIANDO CAPA DE NEGOCIO (RMC + Cancelacion)"
        echo " RMC          : Escuchando en puerto $PUERTO_RMC (hacia $ENDPOINT_PERSISTENCIA)"
        echo " Cancelacion  : Escuchando en puerto $PUERTO_CANCELACION (hacia $ENDPOINT_PERSISTENCIA)"
        echo "================================================================================"

        "$BIN_CANCELACION" "${PUERTO_CANCELACION:-5557}" "$ENDPOINT_PERSISTENCIA" "${2:-2}" 2>&1 | sed 's/^/[CANCELACION] /' &
        PID_CANC=$!

        "$BIN_RMC" "${PUERTO_RMC:-5556}" "$ENDPOINT_PERSISTENCIA" "${1:-4}" 2>&1 | sed 's/^/[RMC] /' &
        PID_RMC=$!

        cleanup_negocio() {
            echo ""
            echo "[-] Deteniendo servicios de negocio (RMC PID $PID_RMC, Cancelacion PID $PID_CANC)..."
            kill "$PID_CANC" "$PID_RMC" 2>/dev/null || true
            wait "$PID_CANC" "$PID_RMC" 2>/dev/null || true
            echo "[+] Capa de negocio detenida."
        }
        trap cleanup_negocio EXIT INT TERM

        wait
        ;;

    gt)
        compilar_si_necesario "$BIN_GT" "$DIR/capa_Servicios-Negocio/servicio_GT/gestorTransacciones.cpp" "-lzmq -lpthread"
        ENDPOINT_RMC="tcp://${IP_RMC:-127.0.0.1}:${PUERTO_RMC:-5556}"
        ENDPOINT_CANCELACION="tcp://${IP_CANCELACION:-127.0.0.1}:${PUERTO_CANCELACION:-5557}"
        echo "================================================================================"
        echo " INICIANDO GESTOR DE TRANSACCIONES (GT)"
        echo " Puerto clientes   : $PUERTO_GT"
        echo " Endpoint RMC      : $ENDPOINT_RMC"
        echo " Endpoint Cancelar : $ENDPOINT_CANCELACION"
        echo "================================================================================"
        exec "$BIN_GT" "${PUERTO_GT:-5555}" "$ENDPOINT_RMC" "$ENDPOINT_CANCELACION" "${1:-4}"
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
