-- schema.sql - Esquema y carga inicial del SRB (Sistema de Reserva de Boletos)
--
-- Uso:
--   createdb srb          (o: CREATE DATABASE srb;)
--   psql -d srb -f schema.sql
--
-- Con Docker (ejemplo):
--   docker exec -i <contenedor> psql -U postgres -d srb < schema.sql
--
-- Es re-ejecutable: borra las tablas y deja la BD en el estado inicial del enunciado.
-- Útil para reiniciar la BD entre escenarios de prueba y antes de la sustentación.
--
-- Estado inicial (enunciado):
--   * 50 eventos (E1..E50) en un mismo mes (noviembre de 2026).
--   * E1..E20 ocurren DOS veces en el mes; E21..E50 solo UNA vez.
--   * Cada ocurrencia tiene 100 asientos, todos libres.
--   => 70 filas en 'evento', 7000 asientos libres, 0 reservas.
--
-- Las columnas y nombres coinciden con las consultas de persistencia.cpp.

BEGIN;

DROP TABLE IF EXISTS reserva;
DROP TABLE IF EXISTS evento;

-- Una fila por OCURRENCIA de un evento (un evento con dos fechas tiene dos filas).
CREATE TABLE evento (
    id_evento   TEXT    NOT NULL,                    -- 'E1', 'E2', ...
    fecha       DATE    NOT NULL,                    -- fecha de esta ocurrencia
    ocurrencias INTEGER NOT NULL CHECK (ocurrencias IN (1, 2)),  -- ocurrencias del evento en el mes
    capacidad   INTEGER NOT NULL DEFAULT 100 CHECK (capacidad > 0),
    libres      INTEGER NOT NULL CHECK (libres >= 0),            -- red de seguridad contra sobreventa
    ocupados    INTEGER NOT NULL CHECK (ocupados >= 0),
    PRIMARY KEY (id_evento, fecha),
    CHECK (libres + ocupados = capacidad)
);

-- Una fila por reserva. La cancelación NO borra: marca estado = 'CANCELADA'.
CREATE TABLE reserva (
    id_reserva      BIGSERIAL PRIMARY KEY,
    id_cliente      TEXT    NOT NULL,
    id_evento       TEXT    NOT NULL,
    fecha           DATE    NOT NULL,
    asientos        INTEGER NOT NULL CHECK (asientos > 0),
    estado          TEXT    NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA', 'CANCELADA')),
    req_id          TEXT    UNIQUE,   -- idPeticion de RESERVAR: idempotencia ante reintentos
    req_cancelacion TEXT,             -- idPeticion de LIBERAR: idempotencia ante reintentos
    FOREIGN KEY (id_evento, fecha) REFERENCES evento (id_evento, fecha)
);

CREATE INDEX idx_reserva_cliente ON reserva (id_cliente);
CREATE INDEX idx_evento_fecha    ON evento (fecha);

-- Carga inicial -------------------------------------------------------------

-- E1..E20: dos ocurrencias (primera quincena y segunda quincena de noviembre).
INSERT INTO evento (id_evento, fecha, ocurrencias, libres, ocupados)
SELECT 'E' || g, DATE '2026-11-01' + ((g - 1) % 14), 2, 100, 0
FROM generate_series(1, 20) AS g;

INSERT INTO evento (id_evento, fecha, ocurrencias, libres, ocupados)
SELECT 'E' || g, DATE '2026-11-16' + ((g - 1) % 14), 2, 100, 0
FROM generate_series(1, 20) AS g;

-- E21..E50: una sola ocurrencia.
INSERT INTO evento (id_evento, fecha, ocurrencias, libres, ocupados)
SELECT 'E' || g, DATE '2026-11-01' + ((g - 1) % 28), 1, 100, 0
FROM generate_series(21, 50) AS g;

COMMIT;

-- Verificación rápida (debe dar: 70 | 50 | 20 | 7000 | 0) --------------------
SELECT count(*)                                    AS ocurrencias,
       count(DISTINCT id_evento)                   AS eventos,
       count(DISTINCT id_evento) FILTER (WHERE ocurrencias = 2) AS eventos_doble,
       sum(libres)                                 AS libres,
       sum(ocupados)                               AS ocupados
FROM evento;
