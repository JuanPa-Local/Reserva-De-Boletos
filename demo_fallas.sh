#!/usr/bin/env bash
# demo_fallas.sh - Demostración de Tolerancia a Fallos y Alta Disponibilidad del SRB
#
# Objetivo:
#   Demostrar que el sistema tolera la caída abrupta (kill -9) de una réplica de RMC
#   y del Gestor de Transacciones (GT) en plena carga de clientes concurrentes,
#   sin que los clientes experimenten errores ni pérdida de peticiones, garantizando
#   la consistencia de la base de datos ("continuar donde se quedó" mediante
#   reintentos idempotentes con el mismo idPeticion).
#
# Principios demostrados:
#   1. Supervisión de procesos: los supervisores reinician procesos caídos en 1 s.
#   2. Ventana de reintentos: el cliente usa Lazy Pirate (3 intentos × 3 s = 9 s),
#      tiempo suficiente para que el supervisor de 1 s reponga el GT.
#   3. Conmutación y balanceo RMC: si RMC#0 muere, el GT redirige a RMC#1.
#   4. Idempotencia en persistencia: el reintento con el mismo idPeticion
#      es deduplicado por la clave única req_id en PostgreSQL.

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
ENDPOINT_RMC="tcp://127.0.0.1:5556,tcp://127.0.0.1:5566"
ENDPOINT_CANCELACION="tcp://127.0.0.1:5557,tcp://127.0.0.1:5567"
ENDPOINT_GT="tcp://127.0.0.1:${PUERTO_GT:-5555}"

BIN_PERSISTENCIA="$DIR/capa_Servicio-Almacenamiento/persistencia"
BIN_CANCELACION="$DIR/capa_Servicios-Negocio/servicio_Cancelacion/cancelacion"
BIN_RMC="$DIR/capa_Servicios-Negocio/servicio_RMC/rmc"
BIN_GT="$DIR/capa_Servicios-Negocio/servicio_GT/gestor"
BIN_CLIENTE="$DIR/capa_Consumidores/cliente"

echo "================================================================================"
echo " SRB - DEMOSTRACIÓN DE TOLERANCIA A FALLOS EN PLENA CARGA"
echo "================================================================================"

# 1. Compilación si es necesario
echo "[-] Verificando compilación de binarios..."
if [ ! -f "$BIN_PERSISTENCIA" ] || [ "$DIR/capa_Servicio-Almacenamiento/persistencia.cpp" -nt "$BIN_PERSISTENCIA" ]; then
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicio-Almacenamiento/persistencia.cpp" -o "$BIN_PERSISTENCIA" -lzmq -lpqxx -lpq -lpthread
fi
if [ ! -f "$BIN_CANCELACION" ] || [ "$DIR/capa_Servicios-Negocio/servicio_Cancelacion/servicioCancelar.cpp" -nt "$BIN_CANCELACION" ]; then
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicios-Negocio/servicio_Cancelacion/servicioCancelar.cpp" -o "$BIN_CANCELACION" -lzmq -lpthread
fi
if [ ! -f "$BIN_RMC" ] || [ "$DIR/capa_Servicios-Negocio/servicio_RMC/rmc.cpp" -nt "$BIN_RMC" ]; then
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicios-Negocio/servicio_RMC/rmc.cpp" -o "$BIN_RMC" -lzmq -lpthread
fi
if [ ! -f "$BIN_GT" ] || [ "$DIR/capa_Servicios-Negocio/servicio_GT/gestorTransacciones.cpp" -nt "$BIN_GT" ]; then
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Servicios-Negocio/servicio_GT/gestorTransacciones.cpp" -o "$BIN_GT" -lzmq -lpthread
fi
if [ ! -f "$BIN_CLIENTE" ] || [ "$DIR/capa_Consumidores/cliente.cpp" -nt "$BIN_CLIENTE" ]; then
    g++ -std=c++17 -Wall -Wextra -O2 "$DIR/capa_Consumidores/cliente.cpp" -o "$BIN_CLIENTE" -lzmq -lpthread
fi
echo "[+] Todos los módulos verificados."

# 2. Contenedor BD y Reset
if ! docker ps | grep -q "$CONTAINER"; then
    echo "[-] Levantando contenedor de base de datos..."
    docker compose --env-file config.env up -d
    sleep 3
fi

echo "[-] Reseteando base de datos a estado inicial (schema_demo.sql)..."
export PGPASSWORD="$PASS"
docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" < "$DIR/schema_demo.sql" > /dev/null 2>&1
echo "[+] BD lista con E1 y E2 (200 asientos libres, 0 reservas)."

# 3. Limpieza de procesos previos
killall -9 persistencia cancelacion rmc gestor cliente 2>/dev/null || true
sleep 1

SUPERVISORES=()
DETENER=0

supervisar_proceso() {
    local etiqueta="$1" pid_file="$2" log_file="$3"
    shift 3
    (
        while [ "$DETENER" -eq 0 ]; do
            "$@" > "$log_file" 2>&1 &
            local hijo=$!
            echo "$hijo" > "$pid_file"
            wait "$hijo" || true
            if [ "$DETENER" -eq 1 ]; then break; fi
            echo -e "[\033[1;33mSUPERVISOR\033[0m] $etiqueta (PID $hijo) terminó inesperadamente. Reiniciando en 1 s..."
            sleep 1
        done
    ) &
    SUPERVISORES+=($!)
}

cleanup() {
    DETENER=1
    echo ""
    echo "[-] Deteniendo supervisores y servicios..."
    kill "${SUPERVISORES[@]}" 2>/dev/null || true
    killall persistencia cancelacion rmc gestor cliente 2>/dev/null || true
    rm -f /tmp/srb_fallas_*.pid
    echo "[+] Limpieza finalizada."
}
trap cleanup EXIT INT TERM

# 4. Iniciar servicios bajo supervisión
echo "[-] Iniciando servicios bajo supervisión automática..."

CADENA_PG="host=127.0.0.1 port=$PUERTO_PG dbname=$DB user=$DB_USER"
CADENA_REPLICA=""
if docker ps --format '{{.Names}}' | grep -q '^srb_postgres_replica$'; then
    CADENA_REPLICA="host=127.0.0.1 port=${PUERTO_POSTGRES_REPLICA:-5435} dbname=$DB user=$DB_USER"
fi

# Persistencia
supervisar_proceso "Persistencia" "/tmp/srb_fallas_persistencia.pid" "/tmp/srb_fallas_persistencia.log" \
    "$BIN_PERSISTENCIA" "${PUERTO_PERSISTENCIA:-5558}" "$CADENA_PG" 4 "$CADENA_REPLICA"

# RMC: 2 réplicas (5556 y 5566)
supervisar_proceso "RMC#0" "/tmp/srb_fallas_rmc0.pid" "/tmp/srb_fallas_rmc0.log" \
    "$BIN_RMC" 5556 "$ENDPOINT_PERSISTENCIA" 4

supervisar_proceso "RMC#1" "/tmp/srb_fallas_rmc1.pid" "/tmp/srb_fallas_rmc1.log" \
    "$BIN_RMC" 5566 "$ENDPOINT_PERSISTENCIA" 4

# Cancelación: 2 réplicas (5557 y 5567)
supervisar_proceso "CANC#0" "/tmp/srb_fallas_canc0.pid" "/tmp/srb_fallas_canc0.log" \
    "$BIN_CANCELACION" 5557 "$ENDPOINT_PERSISTENCIA" 2

supervisar_proceso "CANC#1" "/tmp/srb_fallas_canc1.pid" "/tmp/srb_fallas_canc1.log" \
    "$BIN_CANCELACION" 5567 "$ENDPOINT_PERSISTENCIA" 2

# GT: Gestor de Transacciones
supervisar_proceso "GT" "/tmp/srb_fallas_gt.pid" "/tmp/srb_fallas_gt.log" \
    "$BIN_GT" "${PUERTO_GT:-5555}" "$ENDPOINT_RMC" "$ENDPOINT_CANCELACION" 4

sleep 2
echo "[+] Topología de backend iniciada y supervisada:"
echo "    - Persistencia (PID $(cat /tmp/srb_fallas_persistencia.pid 2>/dev/null || echo '?'))"
echo "    - RMC#0 en :5556 (PID $(cat /tmp/srb_fallas_rmc0.pid 2>/dev/null || echo '?'))"
echo "    - RMC#1 en :5566 (PID $(cat /tmp/srb_fallas_rmc1.pid 2>/dev/null || echo '?'))"
echo "    - CANC#0 en :5557, CANC#1 en :5567"
echo "    - GT en :5555 (PID $(cat /tmp/srb_fallas_gt.pid 2>/dev/null || echo '?'))"

# 5. Generar solicitudes de prueba concurrentes (alta frecuencia)
echo ""
echo "================================================================================"
echo " INICIANDO CARGA CONCURRENTE CON 3 CLIENTES (duración ~16 s)"
echo "================================================================================"

cat << 'EOF' > /tmp/srb_solicitudes_falla1.txt
RESERVAR E1 2026-11-01 2
CONSULTA 11
RESERVAR E1 2026-11-01 2
MODIFICAR ULTIMA 3 -
CONSULTA 11
RESERVAR E2 2026-11-01 4
RESERVAR E1 2026-11-01 2
CONSULTA 11
RESERVAR E2 2026-11-01 3
MODIFICAR ULTIMA 1 -
CONSULTA 11
RESERVAR E1 2026-11-01 2
EOF

cat << 'EOF' > /tmp/srb_solicitudes_falla2.txt
RESERVAR E2 2026-11-01 3
CONSULTA 11
RESERVAR E2 2026-11-01 2
MODIFICAR ULTIMA 4 -
CONSULTA 11
RESERVAR E1 2026-11-01 5
RESERVAR E2 2026-11-01 2
CONSULTA 11
RESERVAR E1 2026-11-01 2
MODIFICAR ULTIMA 1 -
CONSULTA 11
RESERVAR E2 2026-11-01 2
EOF

cat << 'EOF' > /tmp/srb_solicitudes_falla3.txt
CONSULTA 11
RESERVAR E1 2026-11-01 1
CONSULTA 11
RESERVAR E2 2026-11-01 2
CONSULTA 11
RESERVAR E1 2026-11-01 2
MODIFICAR ULTIMA 3 -
CONSULTA 11
RESERVAR E2 2026-11-01 2
CONSULTA 11
RESERVAR E1 2026-11-01 1
CONSULTA 11
EOF

PIDS_CLI=()
"$BIN_CLIENTE" cli_f1 "$ENDPOINT_GT" 127.0.0.1 6051 /tmp/srb_solicitudes_falla1.txt 60 > /tmp/srb_cli_f1.log 2>&1 &
PIDS_CLI+=($!)
"$BIN_CLIENTE" cli_f2 "$ENDPOINT_GT" 127.0.0.1 6052 /tmp/srb_solicitudes_falla2.txt 60 > /tmp/srb_cli_f2.log 2>&1 &
PIDS_CLI+=($!)
"$BIN_CLIENTE" cli_f3 "$ENDPOINT_GT" 127.0.0.1 6053 /tmp/srb_solicitudes_falla3.txt 60 > /tmp/srb_cli_f3.log 2>&1 &
PIDS_CLI+=($!)

echo "[+] 3 clientes enviando solicitudes concurrentes al GT..."

# Esperar 3 segundos para que haya tráfico activo
sleep 3

# ------------------------------------------------------------------------------
# INYECCIÓN DE FALLA 1: Matar réplica RMC#0 con SIGKILL
# ------------------------------------------------------------------------------
PID_RMC0=$(cat /tmp/srb_fallas_rmc0.pid)
echo ""
echo "================================================================================"
echo -e " \033[1;31m[FALLA 1]\033[0m: MATANDO RÉPLICA RMC#0 (PID $PID_RMC0) CON SIGKILL (-9)..."
echo "================================================================================"
kill -9 "$PID_RMC0" 2>/dev/null || true
echo ">> SIGKILL enviado a RMC#0."
echo ">> El GT redirige transparentemente hacia RMC#1 o reintenta."
echo ">> El supervisor detectará la caída y relanzará RMC#0 en 1 s."

# Esperar 4 segundos de tráfico continuo
sleep 4

PID_RMC0_NUEVO=$(cat /tmp/srb_fallas_rmc0.pid)
echo -e ">> Estado RMC#0: \033[1;32mRecuperado por el supervisor\033[0m con nuevo PID: $PID_RMC0_NUEVO"

# ------------------------------------------------------------------------------
# INYECCIÓN DE FALLA 2: Matar Gestor de Transacciones (GT) con SIGKILL
# ------------------------------------------------------------------------------
PID_GT=$(cat /tmp/srb_fallas_gt.pid)
echo ""
echo "================================================================================"
echo -e " \033[1;31m[FALLA 2]\033[0m: MATANDO GESTOR DE TRANSACCIONES (GT, PID $PID_GT) CON SIGKILL (-9)..."
echo "================================================================================"
kill -9 "$PID_GT" 2>/dev/null || true
echo ">> SIGKILL enviado al GT."
echo ">> Los clientes entran en ventana de timeout Lazy Pirate (3 x 3 s = 9 s)."
echo ">> El supervisor del GT reinicia el proceso en 1 s (< 9 s)."
echo ">> Al revivir el GT, los clientes reintentan con el MISMO idPeticion ('continuar donde se quedó')."

# Esperar a que terminen los clientes
echo ""
echo "[-] Esperando a que todos los clientes completen su ejecución..."
for pid in "${PIDS_CLI[@]}"; do
    wait "$pid"
done
echo "[+] Todos los clientes finalizaron sus ráfagas de solicitudes."

PID_GT_NUEVO=$(cat /tmp/srb_fallas_gt.pid)
echo -e ">> Estado GT: \033[1;32mRecuperado por el supervisor\033[0m con nuevo PID: $PID_GT_NUEVO"

# ------------------------------------------------------------------------------
# VERIFICACIÓN DE RESULTADOS Y CONSISTENCIA
# ------------------------------------------------------------------------------
echo ""
echo "================================================================================"
echo " AUDITORÍA DE RESULTADOS Y CONSISTENCIA"
echo "================================================================================"

ERRORES_TIMEOUT=0
OPERACIONES_TOTALES=0

for log in /tmp/srb_cli_f1.log /tmp/srb_cli_f2.log /tmp/srb_cli_f3.log; do
    cli=$(basename "$log" .log)
    errs=$(grep -c "ERROR|Sin respuesta" "$log" || true)
    exitos=$(grep -c "\->  OK|" "$log" || true)
    rechazos=$(grep -c "\->  RECHAZADO|" "$log" || true)
    ERRORES_TIMEOUT=$((ERRORES_TIMEOUT + errs))
    OPERACIONES_TOTALES=$((OPERACIONES_TOTALES + exitos + rechazos))
    echo ">> $cli: $exitos exitosas, $rechazos rechazadas por cupo, $errs errores de timeout."
done

echo ""
echo "Total de operaciones completadas sin fallar: $OPERACIONES_TOTALES"
echo "Total de errores de timeout/desconexión vistos por clientes: $ERRORES_TIMEOUT"

if [ "$ERRORES_TIMEOUT" -gt 0 ]; then
    echo -e "[\033[1;31mFALLO\033[0m] Los clientes experimentaron $ERRORES_TIMEOUT errores de desconexión."
    exit 1
fi
echo -e "[\033[1;32mÉXITO\033[0m] Cero errores vistos por los clientes: el reintento Lazy Pirate y la supervisión operaron a la perfección."

# Verificación en BD
echo ""
echo "--- Verificación de integridad en PostgreSQL ---"
CONSISTENCIA=$(docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -Atc \
"WITH ev AS (SELECT COALESCE(sum(ocupados), 0) AS total_ocupados FROM evento),
      res AS (SELECT COALESCE(sum(asientos), 0) AS total_asientos FROM reserva WHERE estado = 'ACTIVA')
 SELECT CASE WHEN ev.total_ocupados = res.total_asientos THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END FROM ev, res;")

echo ">> Resultado de consistencia en BD: $CONSISTENCIA"
if [ "$CONSISTENCIA" != "CONSISTENTE" ]; then
    echo -e "[\033[1;31mFALLO\033[0m] La base de datos quedó INCONSISTENTE tras las fallas."
    exit 1
fi
echo -e "[\033[1;32mÉXITO\033[0m] Estado de la BD 100% CONSISTENTE: total_ocupados == sum(asientos_activos)."

# Verificar que no hubo duplicados de req_id
DUPLICADOS=$(docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -Atc \
"SELECT count(*) FROM (SELECT req_id FROM reserva WHERE req_id IS NOT NULL GROUP BY req_id HAVING count(*) > 1) d;")
echo ">> Duplicados detectados por req_id: $DUPLICADOS"
if [ "$DUPLICADOS" -ne 0 ]; then
    echo -e "[\033[1;31mFALLO\033[0m] Se detectaron registros duplicados de una misma petición."
    exit 1
fi
echo -e "[\033[1;32mÉXITO\033[0m] Idempotencia verificada: cero reservas duplicadas a pesar de los reintentos."

"$DIR/estado.sh"

echo "================================================================================"
echo " DEMOSTRACIÓN DE TOLERANCIA A FALLOS COMPLETADA EXITOSAMENTE"
echo "================================================================================"
