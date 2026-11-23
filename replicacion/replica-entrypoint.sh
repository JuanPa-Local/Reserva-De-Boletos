#!/bin/bash
# replica-entrypoint.sh - Arranque de la réplica de lectura (standby) de PostgreSQL.
# Primer arranque: espera a la primaria, la clona con pg_basebackup -R (deja standby.signal y
# primary_conninfo) y arranca en modo hot standby. Arranques siguientes: solo arranca.
# Variables: PRIMARY_HOST (por defecto "postgres"), PRIMARY_PORT (5432), REPL_PASSWORD, PGDATA.
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
ORIGEN="${PRIMARY_HOST:-postgres}"
PUERTO="${PRIMARY_PORT:-5432}"
CLAVE="${REPL_PASSWORD:-replica}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "[REPLICA] Esperando a la primaria en $ORIGEN:$PUERTO ..."
    until pg_isready -q -h "$ORIGEN" -p "$PUERTO"; do sleep 1; done
    echo "[REPLICA] Clonando la primaria con pg_basebackup ..."
    mkdir -p "$PGDATA"
    chown postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
    # La clave va en la cadena de conexión para que -R la deje en primary_conninfo.
    gosu postgres pg_basebackup -d "host=$ORIGEN port=$PUERTO user=replicador password=$CLAVE" \
        -D "$PGDATA" -Fp -Xs -R
    echo "[REPLICA] Clon listo; arrancando en modo standby."
fi

exec docker-entrypoint.sh postgres
