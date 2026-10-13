#!/usr/bin/env bash
# lanzar_clientes.sh - Lanza N clientes en paralelo del SRB y recoge sus resumenes
#
# Uso:
#   ./lanzar_clientes.sh <N> [archivo_solicitudes|aleatorio] [tpm] [pctConsulta] [duracion]
# Ejemplos:
#   ./lanzar_clientes.sh 3
#   ./lanzar_clientes.sh 5 aleatorio 30 70 20
#   ./lanzar_clientes.sh 8 archivo.txt 40

set -e

ENV_IP_GT="$IP_GT"
ENV_IP_CLIENTES="$IP_CLIENTES"
ENV_PUERTO_GT="$PUERTO_GT"
ENV_PUERTO_NOTIF_BASE="$PUERTO_NOTIF_BASE"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/config.env" ]; then
    # shellcheck disable=SC1091
    source "$DIR/config.env"
fi

# Respetar variables pasadas por el entorno si fueron especificadas
[ -n "$ENV_IP_GT" ] && IP_GT="$ENV_IP_GT"
[ -n "$ENV_IP_CLIENTES" ] && IP_CLIENTES="$ENV_IP_CLIENTES"
[ -n "$ENV_PUERTO_GT" ] && PUERTO_GT="$ENV_PUERTO_GT"
[ -n "$ENV_PUERTO_NOTIF_BASE" ] && PUERTO_NOTIF_BASE="$ENV_PUERTO_NOTIF_BASE"

ESCENARIO=""
if [ "$1" = "--escenario" ]; then
    ESCENARIO="$2"
    shift 2
    TPM="${1:-300}"
    DURACION="${2:-120}"
elif [[ "$1" == --escenario=* ]]; then
    ESCENARIO="${1#--escenario=}"
    shift 1
    TPM="${1:-300}"
    DURACION="${2:-120}"
fi

if [ -n "$ESCENARIO" ]; then
    if [ "$ESCENARIO" != "3" ] && [ "$ESCENARIO" != "5" ] && [ "$ESCENARIO" != "8" ]; then
        echo "Error: escenario '$ESCENARIO' no valido. Debe ser 3, 5 u 8."
        exit 1
    fi
    N="$ESCENARIO"
    MODO="escenario"
else
    if [ -z "$1" ]; then
        echo "Uso:"
        echo "  $0 <N> [archivo_solicitudes|aleatorio] [tpm=300] [pctConsulta=70] [duracion=120]"
        echo "  $0 --escenario <3|5|8> [tpm=300] [duracion=120]"
        echo "Ejemplos:"
        echo "  $0 3"
        echo "  $0 5 aleatorio 300 70 120"
        echo "  $0 --escenario 3"
        echo "  $0 --escenario 5 300 120"
        echo "  $0 --escenario 8 300 120"
        exit 1
    fi
    N="$1"
    MODO="${2:-aleatorio}"
    TPM="${3:-300}"
    PCT="${4:-70}"
    DURACION="${5:-120}"
fi

ENDPOINT_GT="tcp://${IP_GT:-127.0.0.1}:${PUERTO_GT:-5555}"
IP_CLI="${IP_CLIENTES:-127.0.0.1}"
BASE_NOTIF="${PUERTO_NOTIF_BASE:-6001}"
CLIENTE_BIN="$DIR/capa_Consumidores/cliente"

if [ ! -f "$CLIENTE_BIN" ] || [ "$DIR/capa_Consumidores/cliente.cpp" -nt "$CLIENTE_BIN" ]; then
    echo "Compilando cliente..."
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Consumidores/cliente.cpp" -o "$CLIENTE_BIN" -lzmq -lpthread
fi

echo "================================================================================"
if [ -n "$ESCENARIO" ]; then
    echo " Lanzando Escenario Tabla 1 ($N clientes) hacia GT ($ENDPOINT_GT)"
else
    echo " Lanzando $N clientes concurrentes hacia GT ($ENDPOINT_GT)"
fi
echo " Modo: $MODO | TPM: $TPM | Duracion: ${DURACION}s"
echo "================================================================================"

PIDS=()
LOGS=()

for ((i=1; i<=N; i++)); do
    CLI_ID="cli${i}"
    PUERTO_NOTIF=$((BASE_NOTIF + i))
    LOG_FILE="/tmp/srb_${CLI_ID}.log"
    LOGS+=("$LOG_FILE")
    SEMILLA=$((RANDOM * 100 + i))

    if [ -n "$ESCENARIO" ]; then
        PCT_CONS=100
        PCT_MOD=0
        ROL="Solo consulta"
        if [ "$ESCENARIO" = "3" ]; then
            if [ "$i" -eq 3 ]; then
                PCT_CONS=70
                PCT_MOD=0
                ROL="Consulta y Reserva (70%/30%)"
            fi
        elif [ "$ESCENARIO" = "5" ]; then
            if [ "$i" -ge 4 ]; then
                PCT_CONS=0
                PCT_MOD=50
                ROL="Reserva y Modificación (50%/50%)"
            fi
        elif [ "$ESCENARIO" = "8" ]; then
            if [ "$i" -ge 5 ]; then
                PCT_CONS=0
                PCT_MOD=50
                ROL="Reserva y Modificación (50%/50%)"
            fi
        fi
        "$CLIENTE_BIN" "$CLI_ID" "$ENDPOINT_GT" "$IP_CLI" "$PUERTO_NOTIF" aleatorio "$TPM" "$PCT_CONS" "$DURACION" "$SEMILLA" "$PCT_MOD" > "$LOG_FILE" 2>&1 &
        echo "[-] Cliente $CLI_ID [$ROL] iniciado (PID: $!, puerto notif: $PUERTO_NOTIF, log: $LOG_FILE)"
    elif [ "$MODO" = "aleatorio" ]; then
        "$CLIENTE_BIN" "$CLI_ID" "$ENDPOINT_GT" "$IP_CLI" "$PUERTO_NOTIF" aleatorio "$TPM" "$PCT" "$DURACION" "$SEMILLA" > "$LOG_FILE" 2>&1 &
        echo "[-] Cliente $CLI_ID iniciado (PID: $!, puerto notif: $PUERTO_NOTIF, log: $LOG_FILE)"
    else
        "$CLIENTE_BIN" "$CLI_ID" "$ENDPOINT_GT" "$IP_CLI" "$PUERTO_NOTIF" "$MODO" "$TPM" > "$LOG_FILE" 2>&1 &
        echo "[-] Cliente $CLI_ID iniciado (PID: $!, puerto notif: $PUERTO_NOTIF, log: $LOG_FILE)"
    fi

    PIDS+=($!)
done

echo ""
echo "Esperando a que los $N clientes terminen..."
for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo ""
echo "================================================================================"
echo " RESÚMENES DE EJECUCIÓN ($N CLIENTES)"
echo "================================================================================"

for ((i=1; i<=N; i++)); do
    CLI_ID="cli${i}"
    LOG_FILE="${LOGS[$((i-1))]}"
    echo "----------------------------------------------------------------"
    echo ">> Cliente: $CLI_ID"
    if [ -f "$LOG_FILE" ]; then
        awk '/=== Resumen/{flag=1} flag{print}' "$LOG_FILE"
    else
        echo "No se encontro el archivo de log $LOG_FILE"
    fi
done
echo "================================================================================"
