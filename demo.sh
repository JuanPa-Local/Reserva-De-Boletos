#!/usr/bin/env bash
# demo.sh - Demostracion exacta del SRB en una sola maquina (127.0.0.1)
#
# Pasos de la demo:
#   a) Reset BD y Estado inicial: E1 y E2 con 100 asientos libres.
#   b) Cliente 1 reserva 5 asientos de E1; Cliente 2 reserva 3 de E1 (secuencial).
#   c) Consulta: E1 queda en 92 libres / 8 ocupados (asercion en BD).
#   d) 6 clientes concurrentes reservan 20 asientos cada uno de E2:
#      5 aceptados y 1 rechazado por falta de cupo (asercion en respuestas).
#   e) Consulta final: E2 con 0 libres / 100 ocupados (asercion en BD) y consistencia.

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/config.env" ]; then
    # shellcheck disable=SC1091
    source "$DIR/config.env"
fi

CONTAINER="${CONTAINER_POSTGRES:-srb_postgres}"
DB_USER="${DB_USER:-postgres}"
DB="${DB_NAME:-srb}"
PASS="${DB_PASSWORD:-postgres}"
PUERTO_PG="${PUERTO_POSTGRES:-5434}"

ENDPOINT_PERSISTENCIA="tcp://127.0.0.1:${PUERTO_PERSISTENCIA:-5558}"
ENDPOINT_RMC="tcp://127.0.0.1:${PUERTO_RMC:-5556}"
ENDPOINT_CANCELACION="tcp://127.0.0.1:${PUERTO_CANCELACION:-5557}"
ENDPOINT_GT="tcp://127.0.0.1:${PUERTO_GT:-5555}"

BIN_PERSISTENCIA="$DIR/capa_Servicio-Almacenamiento/persistencia"
BIN_CANCELACION="$DIR/capa_Servicios-Negocio/servicio_Cancelacion/cancelacion"
BIN_RMC="$DIR/capa_Servicios-Negocio/servicio_RMC/rmc"
BIN_GT="$DIR/capa_Servicios-Negocio/servicio_GT/gestor"
BIN_CLIENTE="$DIR/capa_Consumidores/cliente"

pausar() {
    echo ""
    echo ">> Pausa de 2 segundos..."
    sleep 2
    echo ""
}

LOG_LINEAS_P=0
LOG_LINEAS_R=0
LOG_LINEAS_G=0

mostrar_nuevos_logs() {
    local total_p total_r total_g
    total_p=$(wc -l < /tmp/srb_persistencia.log 2>/dev/null || echo 0)
    total_r=$(wc -l < /tmp/srb_rmc.log 2>/dev/null || echo 0)
    total_g=$(wc -l < /tmp/srb_gt.log 2>/dev/null || echo 0)

    echo ""
    echo "--- Nuevas líneas en logs de servicios backend ---"
    echo ">> [LOG Persistencia]:"
    if [ "$total_p" -gt "$LOG_LINEAS_P" ]; then
        sed -n "$((LOG_LINEAS_P + 1)),${total_p}p" /tmp/srb_persistencia.log 2>/dev/null || true
    else
        echo "   (sin actividad nueva)"
    fi
    echo ">> [LOG RMC]:"
    if [ "$total_r" -gt "$LOG_LINEAS_R" ]; then
        sed -n "$((LOG_LINEAS_R + 1)),${total_r}p" /tmp/srb_rmc.log 2>/dev/null || true
    else
        echo "   (sin actividad nueva)"
    fi
    echo ">> [LOG GT]:"
    if [ "$total_g" -gt "$LOG_LINEAS_G" ]; then
        sed -n "$((LOG_LINEAS_G + 1)),${total_g}p" /tmp/srb_gt.log 2>/dev/null || true
    else
        echo "   (sin actividad nueva)"
    fi
    echo "--------------------------------------------------"
    echo ""

    LOG_LINEAS_P=$total_p
    LOG_LINEAS_R=$total_r
    LOG_LINEAS_G=$total_g
}

echo "================================================================================"
echo " SISTEMA DE RESERVA DE BOLETOS (SRB) - DEMOSTRACIÓN OFICIAL"
echo "================================================================================"

# 1. Compilar binarios si hacen falta o si su .cpp es mas nuevo (-nt)
echo "[-] Verificando compilacion de todos los modulos..."
if [ ! -f "$BIN_PERSISTENCIA" ] || [ "$DIR/capa_Servicio-Almacenamiento/persistencia.cpp" -nt "$BIN_PERSISTENCIA" ]; then
    echo "    Compilando persistencia..."
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicio-Almacenamiento/persistencia.cpp" -o "$BIN_PERSISTENCIA" -lzmq -lpqxx -lpq -lpthread
fi
if [ ! -f "$BIN_CANCELACION" ] || [ "$DIR/capa_Servicios-Negocio/servicio_Cancelacion/servicioCancelar.cpp" -nt "$BIN_CANCELACION" ]; then
    echo "    Compilando cancelacion..."
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicios-Negocio/servicio_Cancelacion/servicioCancelar.cpp" -o "$BIN_CANCELACION" -lzmq -lpthread
fi
if [ ! -f "$BIN_RMC" ] || [ "$DIR/capa_Servicios-Negocio/servicio_RMC/rmc.cpp" -nt "$BIN_RMC" ]; then
    echo "    Compilando rmc..."
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicios-Negocio/servicio_RMC/rmc.cpp" -o "$BIN_RMC" -lzmq -lpthread
fi
if [ ! -f "$BIN_GT" ] || [ "$DIR/capa_Servicios-Negocio/servicio_GT/gestorTransacciones.cpp" -nt "$BIN_GT" ]; then
    echo "    Compilando gestor..."
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicios-Negocio/servicio_GT/gestorTransacciones.cpp" -o "$BIN_GT" -lzmq -lpthread
fi
if [ ! -f "$BIN_CLIENTE" ] || [ "$DIR/capa_Consumidores/cliente.cpp" -nt "$BIN_CLIENTE" ]; then
    echo "    Compilando cliente..."
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Consumidores/cliente.cpp" -o "$BIN_CLIENTE" -lzmq -lpthread
fi
echo "[+] Todos los modulos compilados correctamente."

# 2. Asegurar que PostgreSQL está corriendo con docker compose
if ! docker ps | grep -q "$CONTAINER"; then
    echo "[-] Iniciando contenedor de base de datos..."
    docker compose --env-file config.env up -d
    sleep 3
fi

# -----------------------------------------------------------------------------
# PASO A: Reset de BD a estado inicial con schema_demo.sql
# Debe ejecutarse ANTES de iniciar los servicios para que el resumen de arranque
# de Persistencia muestre el estado inicial real.
# -----------------------------------------------------------------------------
echo ""
echo "================================================================================"
echo " PASO A: Reset de BD a estado inicial con schema_demo.sql"
echo "================================================================================"
export PGPASSWORD="$PASS"
docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" < "$DIR/schema_demo.sql" > /dev/null 2>&1
"$DIR/estado.sh"
pausar

# 3. Limpiar procesos previos de servicios
echo "[-] Limpiando instancias previas de servicios..."
killall persistencia cancelacion rmc gestor 2>/dev/null || true
rm -f /tmp/srb_persistencia.log /tmp/srb_cancelacion.log /tmp/srb_rmc.log /tmp/srb_gt.log
LOG_LINEAS_P=0
LOG_LINEAS_R=0
LOG_LINEAS_G=0
sleep 1

# Iniciar servicios en background y configurar limpieza al salir
echo "[-] Iniciando servicios de backend..."
# Cadena de conexion sin password en linea de comandos (libpq usa PGPASSWORD)
CADENA_PG="host=127.0.0.1 port=$PUERTO_PG dbname=$DB user=$DB_USER"

"$BIN_PERSISTENCIA" "${PUERTO_PERSISTENCIA:-5558}" "$CADENA_PG" 4 > /tmp/srb_persistencia.log 2>&1 &
PID_PERSISTENCIA=$!

"$BIN_CANCELACION" "${PUERTO_CANCELACION:-5557}" "$ENDPOINT_PERSISTENCIA" 2 > /tmp/srb_cancelacion.log 2>&1 &
PID_CANCELACION=$!

"$BIN_RMC" "${PUERTO_RMC:-5556}" "$ENDPOINT_PERSISTENCIA" 4 > /tmp/srb_rmc.log 2>&1 &
PID_RMC=$!

"$BIN_GT" "${PUERTO_GT:-5555}" "$ENDPOINT_RMC" "$ENDPOINT_CANCELACION" 4 > /tmp/srb_gt.log 2>&1 &
PID_GT=$!

cleanup() {
    echo ""
    echo "[-] Deteniendo servicios..."
    kill "$PID_GT" "$PID_RMC" "$PID_CANCELACION" "$PID_PERSISTENCIA" 2>/dev/null || true
    echo "[+] Servicios detenidos."
}
trap cleanup EXIT INT TERM

sleep 1
echo "[+] Servicios iniciados: Persistencia ($PID_PERSISTENCIA), Cancelacion ($PID_CANCELACION), RMC ($PID_RMC), GT ($PID_GT)."
mostrar_nuevos_logs
pausar

# -----------------------------------------------------------------------------
# PASO B: Reservas secuenciales de E1 (5 asientos y luego 3 asientos)
# -----------------------------------------------------------------------------
echo "================================================================================"
echo " PASO B: Reservas secuenciales en E1"
echo "         1. Cliente cli_demo1 reserva 5 asientos de E1"
echo "         2. Cliente cli_demo2 reserva 3 asientos de E1"
echo "================================================================================"

echo "RESERVAR E1 2026-11-01 5" > /tmp/srb_demo_b1.txt
echo "RESERVAR E1 2026-11-01 3" > /tmp/srb_demo_b2.txt

echo ">> Ejecutando Cliente cli_demo1 (5 asientos E1)..."
"$BIN_CLIENTE" cli_demo1 "$ENDPOINT_GT" 127.0.0.1 6001 /tmp/srb_demo_b1.txt 60

echo ">> Ejecutando Cliente cli_demo2 (3 asientos E1)..."
"$BIN_CLIENTE" cli_demo2 "$ENDPOINT_GT" 127.0.0.1 6002 /tmp/srb_demo_b2.txt 60
mostrar_nuevos_logs
pausar

# -----------------------------------------------------------------------------
# PASO C: Consulta de E1 (debe tener 92 libres / 8 ocupados)
# -----------------------------------------------------------------------------
echo "================================================================================"
echo " PASO C: Consulta de eventos -> E1 queda en 92 libres / 8 ocupados"
echo "================================================================================"
echo "CONSULTA 11" > /tmp/srb_demo_c.txt
"$BIN_CLIENTE" cli_consulta "$ENDPOINT_GT" 127.0.0.1 6003 /tmp/srb_demo_c.txt 60

# Asercion en Base de Datos para E1
RES_E1=$(docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -Atc "SELECT libres || '/' || ocupados FROM evento WHERE id_evento='E1' AND fecha='2026-11-01';")
echo ">> Verificacion BD E1: $RES_E1 (Esperado: 92/8)"
if [ "$RES_E1" != "92/8" ]; then
    echo -e "[\033[1;31mFALLO\033[0m] E1 deberia estar en 92/8 pero esta en $RES_E1"
    exit 1
fi
echo -e "[\033[1;32mOK\033[0m] Asercion BD E1 superada (92/8)."

"$DIR/estado.sh"
mostrar_nuevos_logs
pausar

# -----------------------------------------------------------------------------
# PASO D: 6 clientes concurrentes reservan 20 asientos cada uno de E2
#         Capacidad total = 100. 5 clientes son aceptados (100 asientos),
#         y 1 cliente es RECHAZADO por falta de cupo.
# -----------------------------------------------------------------------------
echo "================================================================================"
echo " PASO D: Concurrencia - 6 clientes intentan reservar 20 asientos cada uno en E2"
echo "         Esperado: 5 ACEPTADOS (100 asientos) y 1 RECHAZADO (falta de cupo)"
echo "================================================================================"

PIDS_D=()
for k in {1..6}; do
    echo "RESERVAR E2 2026-11-01 20" > "/tmp/srb_demo_d_${k}.txt"
    "$BIN_CLIENTE" "cli_conc_${k}" "$ENDPOINT_GT" 127.0.0.1 "$((6010 + k))" "/tmp/srb_demo_d_${k}.txt" 60 > "/tmp/srb_demo_d_${k}.log" 2>&1 &
    PIDS_D+=($!)
done

for pid in "${PIDS_D[@]}"; do
    wait "$pid"
done

echo ""
echo ">> Resultados de los 6 clientes concurrentes:"
RECHAZADOS=0
ACEPTADOS=0
ERRORES=0
for k in {1..6}; do
    LOG="/tmp/srb_demo_d_${k}.log"
    RESP=$(grep "\->" "$LOG" || true)
    if echo "$RESP" | grep -q "OK|"; then
        echo -e "   [\033[1;32mACEPTADO\033[0m ] Cliente cli_conc_${k}: $RESP"
        ACEPTADOS=$((ACEPTADOS + 1))
    elif echo "$RESP" | grep -q "RECHAZADO"; then
        echo -e "   [\033[1;31mRECHAZADO\033[0m] Cliente cli_conc_${k}: $RESP"
        RECHAZADOS=$((RECHAZADOS + 1))
    else
        echo -e "   [\033[1;33mERROR\033[0m    ] Cliente cli_conc_${k}: $RESP"
        ERRORES=$((ERRORES + 1))
    fi
done

echo ""
echo ">> Resumen concurrencia: $ACEPTADOS aceptado(s), $RECHAZADOS rechazado(s), $ERRORES error(es)."
if [ "$ACEPTADOS" -ne 5 ] || [ "$RECHAZADOS" -ne 1 ] || [ "$ERRORES" -ne 0 ]; then
    echo -e "[\033[1;31mFALLO\033[0m] Concurrencia inesperada: se esperaban 5 aceptados, 1 rechazado y 0 errores."
    exit 1
fi
echo -e "[\033[1;32mOK\033[0m] Asercion de respuestas concurrentes superada (5 aceptados, 1 rechazado)."

mostrar_nuevos_logs
pausar

# -----------------------------------------------------------------------------
# PASO E: Consulta final (E2 debe quedar en 0 libres / 100 ocupados)
# -----------------------------------------------------------------------------
echo "================================================================================"
echo " PASO E: Consulta final y verificación de consistencia"
echo "         E1: 92 libres / 8 ocupados | E2: 0 libres / 100 ocupados"
echo "================================================================================"
echo "CONSULTA 11" > /tmp/srb_demo_e.txt
"$BIN_CLIENTE" cli_final "$ENDPOINT_GT" 127.0.0.1 6020 /tmp/srb_demo_e.txt 60

# Asercion en Base de Datos para E2
RES_E2=$(docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -Atc "SELECT libres || '/' || ocupados FROM evento WHERE id_evento='E2' AND fecha='2026-11-01';")
echo ">> Verificacion BD E2: $RES_E2 (Esperado: 0/100)"
if [ "$RES_E2" != "0/100" ]; then
    echo -e "[\033[1;31mFALLO\033[0m] E2 deberia estar en 0/100 pero esta en $RES_E2"
    exit 1
fi
echo -e "[\033[1;32mOK\033[0m] Asercion BD E2 superada (0/100)."

# Asercion de consistencia de la BD
CONSISTENCIA=$(docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -Atc \
"WITH ev AS (SELECT COALESCE(sum(ocupados), 0) AS total_ocupados FROM evento),
      res AS (SELECT COALESCE(sum(asientos), 0) AS total_asientos FROM reserva WHERE estado = 'ACTIVA')
 SELECT CASE WHEN ev.total_ocupados = res.total_asientos THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END FROM ev, res;")

echo ">> Verificacion de consistencia global: $CONSISTENCIA"
if [ "$CONSISTENCIA" != "CONSISTENTE" ]; then
    echo -e "[\033[1;31mFALLO\033[0m] La base de datos se encuentra INCONSISTENTE."
    exit 1
fi
echo -e "[\033[1;32mOK\033[0m] Asercion de consistencia global superada."

"$DIR/estado.sh"
mostrar_nuevos_logs

echo ""
echo "================================================================================"
echo " DEMO COMPLETADA EXITOSAMENTE"
echo "================================================================================"
