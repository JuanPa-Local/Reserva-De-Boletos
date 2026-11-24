#!/usr/bin/env bash
# demo_escenarios.sh - Demostración de los 4 escenarios de la sustentación del SRB
#
#   Escenario 1  Dos clientes concurrentes: 2 reservas cada uno + varias consultas (resultado visible).
#                BD antes y después.
#   Escenario 2  Dos clientes concurrentes: uno MODIFICA reservas hechas antes (cambio de fecha y de
#                asientos, y un intento que la regla de negocio debe rechazar). BD antes y después.
#   Escenario 3  CANCELACIÓN de una reserva anterior: ACK inmediato + notificación asíncrona al cliente.
#                BD antes y después.
#   Escenario 4  Carga concurrente de 1-2 minutos con muchos clientes; en mitad de la corrida se mata
#                (kill -9) el Gestor de Transacciones y/o el RMC, el supervisor los repone y los clientes
#                siguen enviando peticiones. Verificación automática al final.
#
# Cada escenario termina con VERIFICACIONES automáticas (PASS/FAIL) contra la BD y los logs de los clientes.
# El script sale con código 1 si alguna falla.
#
# BD de demostración (escenarios 1-3): se carga schema.sql y se dejan solo 3 eventos para que se lea bien:
#   E1  (2 ocurrencias: 2026-11-01 y 2026-11-16)   E2  (2026-11-02 y 2026-11-17)   E21 (1 ocurrencia: 2026-11-21)
# El escenario 4 usa la BD completa de rendimiento (schema.sql: 50 eventos, 100 asientos por ocurrencia).
# ¡CADA EJECUCIÓN REINICIA LA BD! (escenario 1 y escenario 4 hacen reset).
#
# Requisitos: el sistema levantado (iniciar_capa.sh persistencia | negocio | gt) con los supervisores,
#   o bien usar --local para que este script lo levante en esta máquina (config.env debe apuntar a ella).
#
# Uso:
#   ./demo_escenarios.sh                         # los 4 escenarios (4 dura ~2 min)
#   ./demo_escenarios.sh --local                 # levanta el sistema en esta máquina y lo apaga al final
#   ./demo_escenarios.sh --escenarios 4 --duracion 120 --clientes 8 --falla ambos
#   ./demo_escenarios.sh --ssh-gt grupo@10.43.100.37 --ssh-negocio grupo@10.43.100.38
#       (cuando GT y RMC corren en otras máquinas: necesario para matarlos en el escenario 4; ssh por llave)
#
# Opciones:
#   --local                    levanta persistencia, negocio y GT con iniciar_capa.sh en esta máquina
#   --escenarios 1,2,3,4       cuáles ejecutar (2 y 3 necesitan haber corrido el 1 en la misma BD)
#   --duracion S               segundos de carga del escenario 4        (defecto 120)
#   --clientes N               clientes concurrentes del escenario 4    (defecto 6)
#   --tpm N                    solicitudes/min por cliente, escenario 4 (defecto 300)
#   --mix C,M,X                % consulta, modificación, cancelación del escenario 4 (defecto 50,15,15; resto reservas)
#   --falla gt|rmc|ambos       qué componente matar en el escenario 4   (defecto ambos: GT al 1/3 y RMC al 2/3)
#   --rmc-todas                matar TODAS las réplicas de RMC a la vez (defecto: una sola)
#   --tpm-demo N               solicitudes/min en los escenarios 1-3    (defecto 60)
#   --ssh-gt USER@HOST         máquina del GT si no es esta
#   --ssh-negocio USER@HOST    máquina de RMC/Cancelación si no es esta
#   --sql-cmd "CMD"            comando que ejecuta psql leyendo SQL por stdin (BD en otra máquina), p. ej.
#                              "ssh grupo@M3 'docker exec -i srb_postgres psql -q -U postgres -d srb'"
#   --si                       no pedir Enter entre escenarios
#   -h, --help                 esta ayuda

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/config.env" ]; then
    # shellcheck disable=SC1091
    source "$DIR/config.env"
fi

# ----------------------------------------------------------------------------- parámetros
LOCAL=0
ESCENARIOS="1,2,3,4"
DURACION=120
CLIENTES=6
TPM=300
MIX="50,15,15"
FALLA="ambos"
RMC_TODAS=0
TPM_DEMO=60
SSH_GT=""
SSH_NEGOCIO=""
SQL_CMD="${SQL_CMD:-}"
SIN_PAUSA=0

while [ $# -gt 0 ]; do
    case "$1" in
        --local) LOCAL=1 ;;
        --escenarios) ESCENARIOS="$2"; shift ;;
        --duracion) DURACION="$2"; shift ;;
        --clientes) CLIENTES="$2"; shift ;;
        --tpm) TPM="$2"; shift ;;
        --mix) MIX="$2"; shift ;;
        --falla) FALLA="$2"; shift ;;
        --rmc-todas) RMC_TODAS=1 ;;
        --tpm-demo) TPM_DEMO="$2"; shift ;;
        --ssh-gt) SSH_GT="$2"; shift ;;
        --ssh-negocio) SSH_NEGOCIO="$2"; shift ;;
        --sql-cmd) SQL_CMD="$2"; shift ;;
        --si) SIN_PAUSA=1 ;;
        -h|--help) sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Opción desconocida: $1 (usa --help)"; exit 2 ;;
    esac
    shift
done

case "$FALLA" in gt|rmc|ambos) ;; *) echo "--falla debe ser gt, rmc o ambos"; exit 2 ;; esac
if [ "$TPM" -lt 20 ] || [ "$TPM_DEMO" -lt 20 ]; then echo "El mínimo del enunciado es 20 solicitudes/min"; exit 2; fi
IFS=',' read -r PCT_CONS PCT_MOD PCT_CANC <<< "$MIX"
if [ -z "$PCT_CANC" ] || [ $((PCT_CONS + PCT_MOD + PCT_CANC)) -gt 100 ]; then echo "--mix inválido (ej. 50,15,15)"; exit 2; fi
quiere() { [[ ",$ESCENARIOS," == *",$1,"* ]]; }

# ----------------------------------------------------------------------------- configuración derivada
IP_GT="${IP_GT:-127.0.0.1}"
PUERTO_GT="${PUERTO_GT:-5555}"
IP_CLIENTES="${IP_CLIENTES:-127.0.0.1}"
BASE_NOTIF="${PUERTO_NOTIF_BASE:-6001}"
PASO="${PASO_REPLICA:-10}"
N_RMC="${REPLICAS_RMC:-1}"
PUERTO_RMC_BASE="${PUERTO_RMC:-5556}"
ENDPOINT_GT="tcp://${IP_GT}:${PUERTO_GT}"
BIN_CLIENTE="$DIR/capa_Consumidores/cliente"
TMP="${TMPDIR:-/tmp}/srb_demo_escenarios"
mkdir -p "$TMP"

CONTAINER="${CONTAINER_POSTGRES:-srb_postgres}"
DB_USER="${DB_USER:-postgres}"
DB="${DB_NAME:-srb}"
export PGPASSWORD="${DB_PASSWORD:-postgres}"

if [ -t 1 ]; then
    NEG=$'\033[1m'; VERDE=$'\033[32m'; ROJO=$'\033[31m'; AMAR=$'\033[33m'; CIAN=$'\033[36m'; MAG=$'\033[35m'; RST=$'\033[0m'
else
    NEG=""; VERDE=""; ROJO=""; AMAR=""; CIAN=""; MAG=""; RST=""
fi

TOTAL_OK=0
TOTAL_FAIL=0
PIDS_LOCAL=()

# ----------------------------------------------------------------------------- utilidades de presentación
linea() { printf '%*s\n' 80 '' | tr ' ' "${1:-=}"; }
titulo() { echo; linea =; echo "${NEG} $1${RST}"; shift; for l in "$@"; do echo "   $l"; done; linea =; }
subtitulo() { echo; echo "${CIAN}${NEG}--- $1 ---${RST}"; }

pausa() {
    if [ "$SIN_PAUSA" -eq 0 ] && [ -t 0 ]; then
        echo; read -r -p "${AMAR}  [Enter para continuar]${RST} " _
    else
        sleep 1
    fi
}

verificar() {  # descripción  esperado  obtenido
    if [ "$2" = "$3" ]; then
        echo "  [${VERDE}PASS${RST}] $1  (esperado=$2, obtenido=$3)"; TOTAL_OK=$((TOTAL_OK + 1))
    else
        echo "  [${ROJO}FAIL${RST}] $1  (esperado=$2, obtenido=$3)"; TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi
}

aviso() { echo "  [${AMAR}AVISO${RST}] $*"; }

# ----------------------------------------------------------------------------- acceso a la BD
detectar_sql() {
    [ -n "$SQL_CMD" ] && return 0
    if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"; then
        SQL_CMD="docker exec -i $CONTAINER psql -q -U $DB_USER -d $DB -v ON_ERROR_STOP=1"
    elif command -v psql >/dev/null 2>&1; then
        SQL_CMD="psql -q -h ${IP_PERSISTENCIA:-127.0.0.1} -p ${PUERTO_POSTGRES:-5434} -U $DB_USER -d $DB -v ON_ERROR_STOP=1"
    else
        echo "${ROJO}No tengo cómo consultar la BD (ni contenedor '$CONTAINER' local ni psql).${RST}"
        echo "Usa --sql-cmd \"ssh usuario@M3 'docker exec -i $CONTAINER psql -q -U $DB_USER -d $DB'\""
        exit 1
    fi
}

sql_tabla() { printf '%s\n' "$1" | eval "$SQL_CMD" 2>&1; }

sql_valor() {  # una sola celda, sin formato
    printf '\\pset tuples_only on\n\\pset format unaligned\n%s\n' "$1" | eval "$SQL_CMD" 2>/dev/null | tr -d '\r' | sed '/^$/d' | head -1
}

reset_bd() {  # completa | demo
    if ! eval "$SQL_CMD" < "$DIR/schema.sql" > /dev/null 2> "$TMP/reset.err"; then
        echo "${ROJO}Falló el reset de la BD:${RST}"; cat "$TMP/reset.err"; exit 1
    fi
    if [ "$1" = "demo" ]; then
        printf "DELETE FROM evento WHERE id_evento NOT IN ('E1','E2','E21');\n" | eval "$SQL_CMD" > /dev/null
    fi
    sleep 2   # deja que la réplica de lectura (si existe) alcance a la primaria
}

SQL_CONSISTENCIA="SELECT sum(libres) AS libres, sum(ocupados) AS ocupados,
       (SELECT count(*) FROM reserva WHERE estado='ACTIVA') AS reservas_activas,
       (SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA') AS asientos_en_reservas,
       CASE WHEN sum(ocupados) = (SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA')
            THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END AS consistencia
FROM evento;"

mostrar_estado() {  # estado completo (BD de demostración, pocas filas)
    subtitulo "BD $1"
    echo "[tabla evento]"
    sql_tabla "SELECT id_evento AS evento, to_char(fecha,'YYYY-MM-DD') AS fecha, ocurrencias AS ocur, capacidad AS cap, libres, ocupados FROM evento ORDER BY length(id_evento), id_evento, fecha;"
    echo "[tabla reserva]"
    sql_tabla "SELECT id_reserva AS id, id_cliente AS cliente, id_evento AS evento, to_char(fecha,'YYYY-MM-DD') AS fecha, asientos, estado FROM reserva ORDER BY id_reserva;"
    echo "[consistencia]"
    sql_tabla "$SQL_CONSISTENCIA"
}

mostrar_resumen() {  # estado compacto (BD completa de 50 eventos)
    subtitulo "BD $1"
    sql_tabla "SELECT count(*) AS ocurrencias, sum(libres) AS libres, sum(ocupados) AS ocupados,
       (SELECT count(*) FROM reserva WHERE estado='ACTIVA') AS reservas_activas,
       (SELECT count(*) FROM reserva WHERE estado='CANCELADA') AS reservas_canceladas,
       CASE WHEN sum(ocupados) = (SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA')
            THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END AS consistencia FROM evento;"
    echo "[ocurrencias con más asientos ocupados]"
    sql_tabla "SELECT id_evento AS evento, to_char(fecha,'YYYY-MM-DD') AS fecha, libres, ocupados FROM evento WHERE ocupados > 0 ORDER BY ocupados DESC, id_evento LIMIT 6;"
}

ocupados_de() { sql_valor "SELECT ocupados FROM evento WHERE id_evento='$1' AND fecha='$2';"; }

# ----------------------------------------------------------------------------- presentación de logs de clientes
# Convierte las líneas del log del cliente en algo legible; las consultas se muestran como tabla.
# mawk (el awk por defecto en Ubuntu) bufferiza la lectura de pipes y la salida en vivo no aparecería hasta el final
AWK_VIVO=""
if awk -W version 2>&1 | head -1 | grep -qi mawk; then AWK_VIVO="-W interactive"; fi

formatear() {  # etiqueta  color
    # shellcheck disable=SC2086
    awk $AWK_VIVO -v et="$1" -v col="$2" -v rst="$RST" '
    function p() { return col "[" et "]" rst " " }
    /  ->  / {
        i = index($0, "  ->  "); req = substr($0, 1, i - 1); resp = substr($0, i + 6); ms = ""
        if (match(resp, /  \([0-9.]+ ms\)[ ]*$/)) {
            ms = substr(resp, RSTART + 3, RLENGTH - 3); sub(/\)[ ]*$/, "", ms); resp = substr(resp, 1, RSTART - 1)
        }
        n = split(req, c, "|"); t = c[1]; out = ""
        if (t == "CONSULTA") {
            out = p() ">> CONSULTA del mes " c[4] "   (" ms ")\n"
            if (resp ~ /^OK\|/) {
                m = split(substr(resp, 4), ev, ";")
                out = out p() sprintf("     %-8s %-12s %8s %10s\n", "evento", "fecha", "libres", "ocupados")
                for (k = 1; k <= m; k++) { split(ev[k], f, ","); out = out p() sprintf("     %-8s %-12s %8s %10s\n", f[1], f[2], f[3], f[4]) }
            } else out = out p() "     " resp "\n"
        } else if (t == "RESERVAR") {
            r = resp; if (resp ~ /^OK\|/) r = "OK -> reserva #" substr(resp, 4)
            out = p() ">> RESERVAR evento=" c[4] " fecha=" c[5] " asientos=" c[6] "  =>  " r "   (" ms ")\n"
        } else if (t == "MODIFICAR") {
            r = resp; if (resp ~ /^OK\|/) r = "OK -> " substr(resp, 4); else if (resp ~ /^RECHAZADO\|/) r = "RECHAZADO -> " substr(resp, 11)
            out = p() ">> MODIFICAR reserva #" c[4] " asientos=" c[5] " nueva_fecha=" c[6] "  =>  " r "   (" ms ")\n"
        } else if (t == "CANCELAR") {
            r = resp; if (resp ~ /^ACK\|/) r = "ACK inmediato -> " substr(resp, 5)
            out = p() ">> CANCELAR reserva #" c[4] "  =>  " r "   (" ms ")\n"
        } else out = p() $0 "\n"
        printf "%s", out; fflush(); next
    }
    /NOTIFICACION/ { printf "%s%s<< %s%s\n", p(), "", $0, ""; fflush(); next }
    /MODO ARCHIVO/ { printf "%s%s%s\n", col, $0, rst; fflush(); next }
    ' 
}

# Ejecuta clientes en archivo concurrentemente y muestra sus logs EN VIVO (intercalados).
# Uso: correr_clientes "id1:puerto1:archivo1:color1" "id2:..." ...
correr_clientes() {
    local pids=() seguidores=() spec id puerto archivo col log
    for spec in "$@"; do
        IFS=':' read -r id puerto archivo col <<< "$spec"
        log="$TMP/${id}.log"; : > "$log"
        "$BIN_CLIENTE" "$id" "$ENDPOINT_GT" "$IP_CLIENTES" "$puerto" "$archivo" "$TPM_DEMO" > "$log" 2>&1 &
        local cpid=$!
        pids+=("$cpid")
        tail -n +1 -f --pid="$cpid" "$log" 2>/dev/null | formatear "$id" "$col" &
        seguidores+=($!)
    done
    echo "  Clientes lanzados en paralelo (PIDs: ${pids[*]})"
    echo
    wait "${pids[@]}" 2>/dev/null
    wait "${seguidores[@]}" 2>/dev/null
}

# ----------------------------------------------------------------------------- sistema: compilación, arranque local, procesos
compilar_cliente() {
    local src="$DIR/capa_Consumidores/cliente.cpp"
    if [ ! -f "$BIN_CLIENTE" ] || [ "$src" -nt "$BIN_CLIENTE" ]; then
        echo "[-] Compilando cliente..."
        g++ -std=c++17 -O2 "$src" -o "$BIN_CLIENTE" -lzmq -lpthread || { echo "${ROJO}No pude compilar el cliente${RST}"; exit 1; }
    fi
}

puerto_abierto() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }

levantar_local() {
    mkdir -p "$TMP/servicios"
    if command -v docker >/dev/null 2>&1 && ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
        echo "[-] Iniciando contenedor de PostgreSQL..."
        (cd "$DIR" && docker compose --env-file config.env up -d >/dev/null 2>&1); sleep 4
    fi
    echo "[-] Levantando persistencia, negocio y GT con iniciar_capa.sh (supervisados)..."
    "$DIR/iniciar_capa.sh" persistencia > "$TMP/servicios/persistencia.log" 2>&1 & PIDS_LOCAL+=($!)
    sleep 2
    "$DIR/iniciar_capa.sh" negocio > "$TMP/servicios/negocio.log" 2>&1 & PIDS_LOCAL+=($!)
    sleep 2
    "$DIR/iniciar_capa.sh" gt > "$TMP/servicios/gt.log" 2>&1 & PIDS_LOCAL+=($!)
    local t
    for t in $(seq 1 30); do puerto_abierto "$IP_GT" "$PUERTO_GT" && break; sleep 1; done
    if ! puerto_abierto "$IP_GT" "$PUERTO_GT"; then
        echo "${ROJO}El GT no quedó escuchando en $IP_GT:$PUERTO_GT. En --local, config.env debe apuntar a esta máquina.${RST}"
        echo "Logs en $TMP/servicios/"; exit 1
    fi
    echo "[+] Sistema local arriba (logs en $TMP/servicios/)"
}

apagar_local() {
    [ "${#PIDS_LOCAL[@]}" -eq 0 ] && return
    echo; echo "[-] Deteniendo el sistema local..."
    kill -TERM "${PIDS_LOCAL[@]}" 2>/dev/null
    sleep 3
    pkill -f '[s]ervicio_RMC/rmc ' 2>/dev/null
    pkill -f '[s]ervicio_Cancelacion/cancelacion ' 2>/dev/null
    pkill -f '[s]ervicio_GT/gestor ' 2>/dev/null
    pkill -f '[c]apa_Servicio-Almacenamiento/persistencia ' 2>/dev/null
    kill -KILL "${PIDS_LOCAL[@]}" 2>/dev/null
}
trap apagar_local EXIT
trap 'echo; echo "[cancelado]"; kill $(jobs -p) 2>/dev/null; exit 130' INT TERM

en_destino() {  # destino(ssh o vacío)  comando...
    local destino="$1"; shift
    if [ -n "$destino" ]; then
        ssh -o BatchMode=yes -o ConnectTimeout=5 "$destino" "$*" 2>/dev/null
    else
        bash -c "$*" 2>/dev/null
    fi
}

# El truco [x]resto evita que pgrep -f se encuentre a sí mismo (o al shell remoto de ssh)
patron_proc() { echo "[${1:0:1}]${1:1} $2 "; }
pids_de() { en_destino "$2" "pgrep -f '$1'" | tr '\n' ' ' | sed 's/ *$//'; }

# ----------------------------------------------------------------------------- escenario 1
reservas_activas_de() { sql_valor "SELECT id_reserva FROM reserva WHERE id_cliente='$1' AND id_evento='$2' AND fecha='$3' AND estado='ACTIVA' ORDER BY id_reserva LIMIT 1;"; }

escenario_1() {
    titulo "ESCENARIO 1: dos clientes concurrentes (reservas + consultas)" \
           "cliA: consulta, reserva 5 en E1, consulta, reserva 2 en E21, consulta" \
           "cliB: reserva 3 en E1, consulta, consulta, reserva 4 en E2, consulta" \
           "Ambos compiten por E1 al mismo tiempo: el sistema no debe sobrevender."
    reset_bd demo
    mostrar_estado "ANTES de la corrida"

    cat > "$TMP/esc1_A.txt" <<EOF
CONSULTA 11
RESERVAR E1 2026-11-01 5
CONSULTA 11
RESERVAR E21 2026-11-21 2
CONSULTA 11
EOF
    cat > "$TMP/esc1_B.txt" <<EOF
RESERVAR E1 2026-11-01 3
CONSULTA 11
CONSULTA 11
RESERVAR E2 2026-11-02 4
CONSULTA 11
EOF
    subtitulo "Ejecución concurrente (salida en vivo de ambos clientes)"
    correr_clientes "cliA:$((BASE_NOTIF + 1)):$TMP/esc1_A.txt:$CIAN" "cliB:$((BASE_NOTIF + 2)):$TMP/esc1_B.txt:$MAG"

    mostrar_estado "DESPUÉS de la corrida"
    subtitulo "Verificaciones del escenario 1"
    verificar "E1 2026-11-01 ocupados (5 de cliA + 3 de cliB)" 8 "$(ocupados_de E1 2026-11-01)"
    verificar "E21 2026-11-21 ocupados (2 de cliA)" 2 "$(ocupados_de E21 2026-11-21)"
    verificar "E2 2026-11-02 ocupados (4 de cliB)" 4 "$(ocupados_de E2 2026-11-02)"
    verificar "reservas activas" 4 "$(sql_valor "SELECT count(*) FROM reserva WHERE estado='ACTIVA';")"
    verificar "consistencia ocupados == asientos de reservas activas" CONSISTENTE \
        "$(sql_valor "SELECT CASE WHEN (SELECT sum(ocupados) FROM evento) = (SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA') THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END;")"
}

# ----------------------------------------------------------------------------- escenario 2
escenario_2() {
    titulo "ESCENARIO 2: modificación de reservas anteriores (con otro cliente activo)" \
           "cliA modifica SUS reservas del escenario 1:" \
           "  - la de E1 (5 asientos): cambia la FECHA 2026-11-01 -> 2026-11-16 conservando los asientos" \
           "  - la de E21 (2 asientos): sube a 6 asientos" \
           "  - la de E21 otra vez, pidiendo cambiar la fecha: debe RECHAZARSE (evento de una sola ocurrencia)" \
           "cliC, a la vez: consulta, reserva 6 en E2 (2026-11-17), consulta, consulta"
    local r_e1 r_e21
    r_e1="$(reservas_activas_de cliA E1 2026-11-01)"
    r_e21="$(reservas_activas_de cliA E21 2026-11-21)"
    if [ -z "$r_e1" ] || [ -z "$r_e21" ]; then
        echo "${ROJO}No encuentro las reservas de cliA del escenario 1. Ejecuta primero el escenario 1.${RST}"; TOTAL_FAIL=$((TOTAL_FAIL + 1)); return
    fi
    echo "  Reservas a modificar (tomadas de la BD): #$r_e1 (E1) y #$r_e21 (E21)"
    mostrar_estado "ANTES de la corrida"

    cat > "$TMP/esc2_A.txt" <<EOF
MODIFICAR $r_e1 5 2026-11-16
MODIFICAR $r_e21 6 -
MODIFICAR $r_e21 6 2026-11-16
CONSULTA 11
EOF
    cat > "$TMP/esc2_C.txt" <<EOF
CONSULTA 11
RESERVAR E2 2026-11-17 6
CONSULTA 11
CONSULTA 11
EOF
    subtitulo "Ejecución concurrente (salida en vivo de ambos clientes)"
    correr_clientes "cliA:$((BASE_NOTIF + 1)):$TMP/esc2_A.txt:$CIAN" "cliC:$((BASE_NOTIF + 3)):$TMP/esc2_C.txt:$MAG"

    mostrar_estado "DESPUÉS de la corrida"
    subtitulo "Verificaciones del escenario 2"
    verificar "reserva #$r_e1 cambió de fecha" 2026-11-16 "$(sql_valor "SELECT to_char(fecha,'YYYY-MM-DD') FROM reserva WHERE id_reserva=$r_e1;")"
    verificar "E1 2026-11-01 ocupados (quedan solo los 3 de cliB)" 3 "$(ocupados_de E1 2026-11-01)"
    verificar "E1 2026-11-16 ocupados (los 5 movidos)" 5 "$(ocupados_de E1 2026-11-16)"
    verificar "reserva #$r_e21 ahora tiene asientos" 6 "$(sql_valor "SELECT asientos FROM reserva WHERE id_reserva=$r_e21;")"
    verificar "E21 2026-11-21 ocupados" 6 "$(ocupados_de E21 2026-11-21)"
    verificar "cambio de fecha en evento de una ocurrencia fue RECHAZADO" 1 \
        "$(grep -c "^MODIFICAR|.*|${r_e21}|6|2026-11-16  ->  RECHAZADO" "$TMP/cliA.log")"
    verificar "E2 2026-11-17 ocupados (6 de cliC)" 6 "$(ocupados_de E2 2026-11-17)"
    verificar "consistencia ocupados == asientos de reservas activas" CONSISTENTE \
        "$(sql_valor "SELECT CASE WHEN (SELECT sum(ocupados) FROM evento) = (SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA') THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END;")"
}

# ----------------------------------------------------------------------------- escenario 3
escenario_3() {
    titulo "ESCENARIO 3: cancelación (asíncrona) de una reserva anterior" \
           "cliB cancela su reserva de E2 (2026-11-02, 4 asientos)." \
           "El GT responde ACK al instante; el servicio de Cancelación la procesa aparte y" \
           "NOTIFICA al cliente directamente cuando termina."
    local r_cancel
    r_cancel="$(reservas_activas_de cliB E2 2026-11-02)"
    if [ -z "$r_cancel" ]; then
        echo "${ROJO}No encuentro la reserva activa de cliB en E2. Ejecuta primero el escenario 1.${RST}"; TOTAL_FAIL=$((TOTAL_FAIL + 1)); return
    fi
    echo "  Reserva a cancelar (tomada de la BD): #$r_cancel"
    mostrar_estado "ANTES de la corrida"

    cat > "$TMP/esc3_B.txt" <<EOF
CANCELAR $r_cancel
CONSULTA 11
CONSULTA 11
EOF
    subtitulo "Ejecución (observa el ACK inmediato y, después, la NOTIFICACIÓN asíncrona)"
    correr_clientes "cliB:$((BASE_NOTIF + 2)):$TMP/esc3_B.txt:$MAG"

    mostrar_estado "DESPUÉS de la corrida"
    subtitulo "Verificaciones del escenario 3"
    verificar "el GT respondió ACK a la cancelación" 1 "$(grep -c "^CANCELAR|.*  ->  ACK|" "$TMP/cliB.log")"
    verificar "el cliente recibió la notificación asíncrona OK" 1 "$(grep -c "NOTIFICACION.*|OK|" "$TMP/cliB.log")"
    verificar "reserva #$r_cancel quedó CANCELADA" CANCELADA "$(sql_valor "SELECT estado FROM reserva WHERE id_reserva=$r_cancel;")"
    verificar "E2 2026-11-02 ocupados (los 4 asientos se liberaron)" 0 "$(ocupados_de E2 2026-11-02)"
    verificar "consistencia ocupados == asientos de reservas activas" CONSISTENTE \
        "$(sql_valor "SELECT CASE WHEN (SELECT sum(ocupados) FROM evento) = (SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA') THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END;")"
}

# ----------------------------------------------------------------------------- escenario 4
contar_logs() {  # patrón-grep... sobre los logs de carga
    cat "$TMP"/carga*.log 2>/dev/null | grep -c "$1"
}

escenario_4() {
    titulo "ESCENARIO 4: carga concurrente con falla de componentes en mitad de la corrida" \
           "$CLIENTES clientes x $TPM solicitudes/min durante $DURACION s (mezcla ${PCT_CONS}% consulta / ${PCT_MOD}% modificación / ${PCT_CANC}% cancelación / $((100 - PCT_CONS - PCT_MOD - PCT_CANC))% reserva)" \
           "Falla a inyectar (kill -9): $FALLA$([ "$RMC_TODAS" -eq 1 ] && echo ' (todas las réplicas de RMC)')" \
           "Los clientes NO se detienen: deben seguir enviando peticiones durante la falla."

    # --- localizar procesos a matar
    local gt_pat rmc_pats=() canc_pat i p
    gt_pat="$(patron_proc servicio_GT/gestor "$PUERTO_GT")"
    for ((i = 0; i < N_RMC; i++)); do rmc_pats+=("$(patron_proc servicio_RMC/rmc $((PUERTO_RMC_BASE + i * PASO)))"); done
    local faltan=0
    echo "  Procesos localizados:"
    if [ "$FALLA" != "rmc" ]; then
        p="$(pids_de "$gt_pat" "$SSH_GT")"; echo "    GT        PID: ${p:-${ROJO}NO ENCONTRADO${RST}}"; [ -z "$p" ] && faltan=1
    fi
    if [ "$FALLA" != "gt" ]; then
        for ((i = 0; i < N_RMC; i++)); do
            p="$(pids_de "${rmc_pats[$i]}" "$SSH_NEGOCIO")"; echo "    RMC#$i     PID: ${p:-${ROJO}NO ENCONTRADO${RST}}"; [ -z "$p" ] && faltan=1
        done
    fi
    if [ "$faltan" -eq 1 ]; then
        echo "${ROJO}No encuentro algún proceso a matar. Si GT/RMC corren en otras máquinas usa --ssh-gt y --ssh-negocio;"
        echo "si corren supervisados, deben haberse levantado con ./iniciar_capa.sh.${RST}"
        TOTAL_FAIL=$((TOTAL_FAIL + 1)); return
    fi

    reset_bd completa
    mostrar_resumen "ANTES de la corrida"

    # --- lanzar clientes
    rm -f "$TMP"/carga*.log
    local pids=() logs=()
    for ((i = 1; i <= CLIENTES; i++)); do
        "$BIN_CLIENTE" "carga$i" "$ENDPOINT_GT" "$IP_CLIENTES" "$((BASE_NOTIF + 10 + i))" aleatorio "$TPM" "$PCT_CONS" \
            "$DURACION" "$((7000 + i))" "$PCT_MOD" "$PCT_CANC" > "$TMP/carga$i.log" 2>&1 &
        pids+=($!)
    done
    echo; echo "  ${CLIENTES} clientes lanzados en paralelo (PIDs: ${pids[*]})"

    # --- calendario de fallas
    local ev_t=() ev_nombre=() ev_tipo=() ev_hecho=()
    case "$FALLA" in
        gt)    ev_t=($((DURACION / 2)));                  ev_nombre=("Caída del Gestor de Transacciones (GT)"); ev_tipo=(gt) ;;
        rmc)   ev_t=($((DURACION / 2)));                  ev_nombre=("Caída del RMC");                          ev_tipo=(rmc) ;;
        ambos) ev_t=($((DURACION / 3)) $((2 * DURACION / 3))); ev_nombre=("Caída del Gestor de Transacciones (GT)" "Caída del RMC"); ev_tipo=(gt rmc) ;;
    esac
    ev_hecho=(0 0)
    local resultados_recup=() post_t=() post_base=() post_nombre=()

    local inicio=$SECONDS t proximo_estado=0 vivos total errores e
    while :; do
        vivos=0; for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && vivos=$((vivos + 1)); done
        [ "$vivos" -eq 0 ] && break
        t=$((SECONDS - inicio))

        # inyección de fallas
        for ((e = 0; e < ${#ev_t[@]}; e++)); do
            if [ "${ev_hecho[$e]}" -eq 0 ] && [ "$t" -ge "${ev_t[$e]}" ]; then
                ev_hecho[$e]=1
                echo; echo "${ROJO}${NEG}>>> [t=${t}s] ${ev_nombre[$e]}: kill -9${RST}"
                local antes_total; antes_total="$(contar_logs '  ->  ')"
                local objetivos=() destinos=() nombres=()
                if [ "${ev_tipo[$e]}" = "gt" ]; then
                    objetivos=("$gt_pat"); destinos=("$SSH_GT"); nombres=("GT")
                else
                    if [ "$RMC_TODAS" -eq 1 ]; then
                        for ((i = 0; i < N_RMC; i++)); do objetivos+=("${rmc_pats[$i]}"); destinos+=("$SSH_NEGOCIO"); nombres+=("RMC#$i"); done
                    else
                        objetivos=("${rmc_pats[0]}"); destinos=("$SSH_NEGOCIO"); nombres=("RMC#0")
                    fi
                fi
                local viejos=() k
                for ((k = 0; k < ${#objetivos[@]}; k++)); do
                    viejos[$k]="$(pids_de "${objetivos[$k]}" "${destinos[$k]}")"
                    echo "    kill -9 ${nombres[$k]} (PID ${viejos[$k]})"
                    en_destino "${destinos[$k]}" "kill -9 ${viejos[$k]}" >/dev/null
                done
                local t0 t1 pendientes=${#objetivos[@]} hechos=()
                t0="$(date +%s.%N)"
                while [ "$pendientes" -gt 0 ] && [ "$(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN{print (b-a>30)}')" -eq 0 ]; do
                    for ((k = 0; k < ${#objetivos[@]}; k++)); do
                        [ -n "${hechos[$k]:-}" ] && continue
                        local nuevos; nuevos="$(pids_de "${objetivos[$k]}" "${destinos[$k]}")"
                        if [ -n "$nuevos" ] && [ "$nuevos" != "${viejos[$k]}" ]; then
                            t1="$(date +%s.%N)"; hechos[$k]=1; pendientes=$((pendientes - 1))
                            local dur; dur="$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')"
                            echo "    ${VERDE}${nombres[$k]} repuesto por el supervisor (PID $nuevos) en ${dur} s${RST}"
                            resultados_recup+=("${nombres[$k]}=${dur}")
                        fi
                    done
                    sleep 0.1
                done
                for ((k = 0; k < ${#objetivos[@]}; k++)); do
                    [ -z "${hechos[$k]:-}" ] && { echo "    ${ROJO}${nombres[$k]} NO fue repuesto${RST}"; resultados_recup+=("${nombres[$k]}=NO"); }
                done
                post_t+=($((SECONDS - inicio + 5))); post_base+=("$antes_total"); post_nombre+=("${ev_nombre[$e]}")
            fi
        done

        # comprobación "la carga continúa" 5 s después de cada falla
        for ((e = 0; e < ${#post_t[@]}; e++)); do
            if [ -n "${post_t[$e]}" ] && [ "$t" -ge "${post_t[$e]}" ]; then
                total="$(contar_logs '  ->  ')"; errores="$(contar_logs '  ->  ERROR')"
                echo "    ${VERDE}La carga continuó: +$((total - post_base[e])) solicitudes completadas en los 5 s posteriores a la falla (errores acumulados: $errores)${RST}"
                post_t[$e]=""
            fi
        done

        # estado periódico
        if [ "$t" -ge "$proximo_estado" ]; then
            total="$(contar_logs '  ->  ')"; errores="$(contar_logs '  ->  ERROR')"
            local marca="${VERDE}errores=$errores${RST}"; [ "$errores" -gt 0 ] && marca="${ROJO}errores=$errores${RST}"
            printf '   [t=%3ss/%ss] clientes activos=%s  solicitudes completadas=%s  %s\n' "$t" "$DURACION" "$vivos" "$total" "$marca"
            proximo_estado=$((t + 5))
        fi
        sleep 1
    done
    echo; echo "[carga] clientes terminados; esperando 3 s por notificaciones pendientes..."; sleep 3

    # --- resultados
    subtitulo "Resumen que imprimió cada cliente"
    for ((i = 1; i <= CLIENTES; i++)); do
        echo "  carga$i:"; awk '/=== Resumen/{f=1} f&&NF{print "    " $0}' "$TMP/carga$i.log"
    done
    mostrar_resumen "DESPUÉS de la corrida"

    subtitulo "Verificaciones del escenario 4"
    local n_total n_err n_res_ok n_res_bd n_ack n_notif n_lentas max_ms
    n_total="$(contar_logs '  ->  ')"
    n_err="$(contar_logs '  ->  ERROR')"
    n_res_ok="$(cat "$TMP"/carga*.log | grep -c '^RESERVAR|.*  ->  OK|')"
    n_res_bd="$(sql_valor "SELECT count(*) FROM reserva;")"
    n_ack="$(cat "$TMP"/carga*.log | grep -c '^CANCELAR|.*  ->  ACK|')"
    n_notif="$(cat "$TMP"/carga*.log | grep -c 'NOTIFICACION')"
    read -r n_lentas max_ms < <(cat "$TMP"/carga*.log | grep '  ->  ' | grep -v '  ->  ERROR' | awk '
        match($0, /\([0-9.]+ ms\)[ ]*$/) { v = substr($0, RSTART + 1); sub(/ ms\).*$/, "", v); v += 0; if (v > 1000) l++; if (v > m) m = v }
        END { printf "%d %.0f\n", l, m }')
    echo "  Solicitudes completadas: $n_total | con latencia > 1 s (absorbidas por reintento): $n_lentas | latencia máxima: ${max_ms} ms"
    echo "  Tiempos de reposición: ${resultados_recup[*]:-—}"
    if [ "$DURACION" -lt 60 ]; then aviso "la corrida duró ${DURACION} s; el enunciado pide que dure al menos 1-2 minutos (usa --duracion 120)"; fi
    verificar "las fallas fueron transparentes: respuestas ERROR en los clientes" 0 "$n_err"
    verificar "todos los componentes caídos fueron repuestos" 0 "$(printf '%s\n' "${resultados_recup[@]}" | grep -c '=NO$')"
    verificar "reservas OK vistas por los clientes == filas en la BD (sin duplicados ni pérdidas)" "$n_res_ok" "$n_res_bd"
    verificar "consistencia ocupados == asientos de reservas activas" CONSISTENTE \
        "$(sql_valor "SELECT CASE WHEN (SELECT sum(ocupados) FROM evento) = (SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA') THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END;")"
    if [ "$n_ack" -eq "$n_notif" ]; then
        verificar "cancelaciones: ACK == notificaciones recibidas" "$n_ack" "$n_notif"
    else
        aviso "cancelaciones: $n_ack ACK vs $n_notif notificaciones (esta prueba no mata Cancelación; revisa si faltó tiempo de espera)"
    fi
}

# ----------------------------------------------------------------------------- principal
detectar_sql
compilar_cliente
[ "$LOCAL" -eq 1 ] && levantar_local

titulo "SRB - DEMOSTRACIÓN DE ESCENARIOS" \
       "GT: $ENDPOINT_GT | clientes desde: $IP_CLIENTES | escenarios: $ESCENARIOS" \
       "BD: $(echo "$SQL_CMD" | cut -c1-70)"
if ! puerto_abierto "$IP_GT" "$PUERTO_GT"; then
    echo "${ROJO}El GT no responde en $IP_GT:$PUERTO_GT. Levanta el sistema (o usa --local).${RST}"; exit 1
fi
if [ -z "$(sql_valor 'SELECT 1;')" ]; then
    echo "${ROJO}No puedo consultar la BD con: $SQL_CMD${RST}"; exit 1
fi

quiere 1 && { escenario_1; pausa; }
quiere 2 && { escenario_2; pausa; }
quiere 3 && { escenario_3; pausa; }
quiere 4 && { escenario_4; }

echo; linea =
if [ "$TOTAL_FAIL" -eq 0 ]; then
    echo "${VERDE}${NEG} RESULTADO FINAL: $TOTAL_OK verificaciones PASS, 0 FAIL${RST}"
else
    echo "${ROJO}${NEG} RESULTADO FINAL: $TOTAL_OK PASS, $TOTAL_FAIL FAIL${RST}"
fi
linea =
[ "$TOTAL_FAIL" -eq 0 ]
