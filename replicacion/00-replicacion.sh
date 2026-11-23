#!/bin/bash
# 00-replicacion.sh - Se ejecuta UNA vez, al crear la BD primaria (docker-entrypoint-initdb.d).
# Crea el usuario de replicación y autoriza a las réplicas a conectarse a la primaria.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE replicador WITH REPLICATION LOGIN PASSWORD '${REPL_PASSWORD:-replica}';
EOSQL

echo "host replication replicador all scram-sha-256" >> "$PGDATA/pg_hba.conf"
