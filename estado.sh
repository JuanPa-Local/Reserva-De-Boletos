#!/usr/bin/env bash
# estado.sh - Muestra el estado de la BD del SRB y la verificacion de consistencia

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/config.env" ]; then
    # shellcheck disable=SC1091
    source "$DIR/config.env"
fi

CONTAINER="${CONTAINER_POSTGRES:-srb_postgres}"
DB_USER="${DB_USER:-postgres}"
DB="${DB_NAME:-srb}"

echo "================================================================================"
echo " ESTADO DE LA BASE DE DATOS (Contenedor: $CONTAINER, BD: $DB)"
echo "================================================================================"

echo ""
echo "--- TABLA: evento ---"
docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -c \
"SELECT id_evento, to_char(fecha, 'YYYY-MM-DD') AS fecha, ocurrencias, capacidad, libres, ocupados FROM evento ORDER BY length(id_evento), id_evento, fecha;"

echo ""
echo "--- RESERVAS ACTIVAS ---"
docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -c \
"SELECT id_reserva, id_cliente, id_evento, to_char(fecha, 'YYYY-MM-DD') AS fecha, asientos, estado, req_id FROM reserva WHERE estado = 'ACTIVA' ORDER BY id_reserva;"

echo ""
echo "--- VERIFICACION DE CONSISTENCIA ---"
docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB" -c \
"WITH ev AS (
    SELECT count(*) AS total_ocurrencias,
           COALESCE(sum(libres), 0) AS total_libres,
           COALESCE(sum(ocupados), 0) AS total_ocupados
    FROM evento
), res AS (
    SELECT count(*) AS total_reservas_activas,
           COALESCE(sum(asientos), 0) AS total_asientos_activos
    FROM reserva
    WHERE estado = 'ACTIVA'
)
SELECT ev.total_ocurrencias,
       ev.total_libres,
       ev.total_ocupados,
       res.total_reservas_activas,
       res.total_asientos_activos,
       CASE WHEN ev.total_ocupados = res.total_asientos_activos THEN 'CONSISTENTE' ELSE 'INCONSISTENTE' END AS consistencia
FROM ev, res;"
echo "================================================================================"
