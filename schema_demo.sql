-- schema_demo.sql - Esquema y carga para la demo del SRB (2 eventos)
--
-- Uso:
--   psql -d srb -f schema_demo.sql
-- Con Docker:
--   docker exec -i <contenedor> psql -U postgres -d srb < schema_demo.sql
--
-- Es re-ejecutable: borra las tablas y deja la BD en el estado inicial de la demo.

BEGIN;

DROP TABLE IF EXISTS reserva;
DROP TABLE IF EXISTS evento;

CREATE TABLE evento (
    id_evento   TEXT    NOT NULL,                    -- 'E1', 'E2'
    fecha       DATE    NOT NULL,                    -- fecha de esta ocurrencia
    ocurrencias INTEGER NOT NULL CHECK (ocurrencias IN (1, 2)),
    capacidad   INTEGER NOT NULL DEFAULT 100 CHECK (capacidad > 0),
    libres      INTEGER NOT NULL CHECK (libres >= 0),
    ocupados    INTEGER NOT NULL CHECK (ocupados >= 0),
    PRIMARY KEY (id_evento, fecha),
    CHECK (libres + ocupados = capacidad)
);

CREATE TABLE reserva (
    id_reserva      BIGSERIAL PRIMARY KEY,
    id_cliente      TEXT    NOT NULL,
    id_evento       TEXT    NOT NULL,
    fecha           DATE    NOT NULL,
    asientos        INTEGER NOT NULL CHECK (asientos > 0),
    estado          TEXT    NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA', 'CANCELADA')),
    req_id          TEXT    UNIQUE,
    req_cancelacion TEXT,
    FOREIGN KEY (id_evento, fecha) REFERENCES evento (id_evento, fecha)
);

CREATE INDEX idx_reserva_cliente ON reserva (id_cliente);
CREATE INDEX idx_evento_fecha    ON evento (fecha);

-- Carga inicial: SOLO 2 eventos (E1, E2), 1 ocurrencia cada uno, 100 libres, 0 ocupados.
INSERT INTO evento (id_evento, fecha, ocurrencias, capacidad, libres, ocupados)
VALUES
    ('E1', DATE '2026-11-01', 1, 100, 100, 0),
    ('E2', DATE '2026-11-01', 1, 100, 100, 0);

COMMIT;

-- Verificación rápida (debe dar: 2 ocurrencias | 2 eventos | 200 libres | 0 ocupados)
SELECT count(*)                                    AS ocurrencias,
       count(DISTINCT id_evento)                   AS eventos,
       sum(libres)                                 AS libres,
       sum(ocupados)                               AS ocupados
FROM evento;
