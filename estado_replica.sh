#!/usr/bin/env bash
# estado_replica.sh - Muestra el estado de la replicación PostgreSQL (primaria -> réplica de lectura)
# Ejecutar en M3 (donde corren los contenedores).

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/config.env" ]; then
    # shellcheck disable=SC1091
    source "$DIR/config.env"
fi

PRIMARIA="${CONTAINER_POSTGRES:-srb_postgres}"
REPLICA="${CONTAINER_POSTGRES_REPLICA:-srb_postgres_replica}"
DB_USER="${DB_USER:-postgres}"
DB="${DB_NAME:-srb}"
TOTALES="SELECT sum(libres) AS libres, sum(ocupados) AS ocupados, (SELECT count(*) FROM reserva WHERE estado='ACTIVA') AS reservas_activas FROM evento;"

echo "================================================================================"
echo " REPLICACIÓN: primaria ($PRIMARIA) -> réplica ($REPLICA)"
echo "================================================================================"
echo ""
echo "--- Primaria: réplicas conectadas ---"
docker exec -i "$PRIMARIA" psql -U "$DB_USER" -d "$DB" -c \
"SELECT client_addr, state, sync_state, replay_lag FROM pg_stat_replication;"
echo "--- Primaria: totales ---"
docker exec -i "$PRIMARIA" psql -U "$DB_USER" -d "$DB" -c "$TOTALES"
echo "--- Réplica: ¿está en recuperación (solo lectura)? y totales ---"
docker exec -i "$REPLICA" psql -U "$DB_USER" -d "$DB" -c "SELECT pg_is_in_recovery() AS es_replica;"
docker exec -i "$REPLICA" psql -U "$DB_USER" -d "$DB" -c "$TOTALES"
echo "================================================================================"
