# Sistema de Reserva de Boletos (SRB)
**Introducción a Sistemas Distribuidos — Pontificia Universidad Javeriana (PUJ 2026-30)**

Sistema distribuido de alta concurrencia desarrollado en **C++17**, **ZeroMQ** y **PostgreSQL** (`libpqxx`) para la gestión de reservas, cancelaciones, modificaciones y consultas de eventos y boletos sin sobreventa ni inconsistencias.

---

## 1. Arquitectura y Topología de Red

El sistema desacopla sus responsabilidades en 4 capas distribuidas en 3 máquinas virtuales/físicas:

```
                      +-----------------------------+
                      |       CLIENTES (M1 / CLI)   |
                      |  - REQ a GT:5555            |
                      |  - PULL notificaciones:600X |
                      +--------------+--------------+
                                     |
                                     v REQ -> ROUTER
                      +-----------------------------+
                      |   MÁQUINA 1: GESTOR (GT)    |
                      |  - ROUTER escucha en :5555  |
                      |  - Canal Síncrono -> RMC    |
                      |  - Canal Asíncrono -> Canc. |
                      +-------+-------------+-------+
                              |             |
           REQ -> ROUTER/REP  |             | PUSH -> PULL
                              v             v
       +----------------------------+   +----------------------------+
       |     MÁQUINA 2: NEGOCIO     |   |     MÁQUINA 2: NEGOCIO     |
       |       Servicio RMC         |   |    Servicio Cancelación    |
       |  - Escucha en :5556        |   |  - Escucha en :5557        |
       |  - Lazy Pirate a Persist.  |   |  - Notifica a PULL cliente |
       +--------------+-------------+   +-------------+--------------+
                      |                               |
                      | REQ -> ROUTER                 | REQ -> ROUTER
                      +---------------+---------------+
                                      |
                                      v
                      +-----------------------------+
                      |  MÁQUINA 3: ALMACENAMIENTO   |
                      |   Servicio Persistencia     |
                      |  - ROUTER escucha en :5558  |
                      |  - Hilos libpqxx directos   |
                      |  - Contenedor PostgreSQL    |
                      |    (Puerto :5434 -> :5432)  |
                      +-----------------------------+
```

### Puertos a abrir en Firewalls / Red

| Máquina | Rol | Puertos a abrir | Protocolo | Descripción |
|---|---|---|---|---|
| **M1** | Gestor de Transacciones (GT) | `5555` | TCP | Entrada de clientes a GT |
| **M2** | Capa de Negocio | `5556`, `5557` | TCP | `5556`: RMC (síncrono)<br>`5557`: Cancelación (asíncrono) |
| **M3** | Capa de Almacenamiento | `5558`, `5434` | TCP | `5558`: Persistencia ZeroMQ<br>`5434`: PostgreSQL Docker (mapeado) |
| **Clientes** | Clientes consumidores | `6001` en adelante | TCP | PULL para recibir notificaciones asíncronas de cancelación |

---

## 2. Requisitos y Dependencias

Para compilar y ejecutar el sistema en Linux (Ubuntu/Debian):

```bash
sudo apt-get update
sudo apt-get install -y build-essential g++ libzmq3-dev cppzmq-dev libpqxx-dev libpq-dev docker.io docker-compose-plugin zip
```

En distribuciones basadas en RPM (Fedora/RHEL):
```bash
sudo dnf install -y gcc-c++ zeromq-devel cppzmq-devel libpqxx-devel libpq-devel podman podman-compose zip
```

---

## 3. Configuración del Entorno (`config.env`)

El archivo [config.env](file:///home/silvaaa/Documents/uni/5to/distri/proyecto/Reserva-De-Boletos/config.env) centraliza las IPs, puertos y credenciales:

```env
IP_CLIENTES=10.43.99.220
IP_GT=10.43.100.37
IP_RMC=10.43.100.38
IP_CANCELACION=10.43.100.38
IP_PERSISTENCIA=10.43.99.215

PUERTO_GT=5555
PUERTO_RMC=5556
PUERTO_CANCELACION=5557
PUERTO_PERSISTENCIA=5558
PUERTO_POSTGRES=5434
PUERTO_NOTIF_BASE=6001

DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=srb
CONTAINER_POSTGRES=srb_postgres
```

---

## 4. Base de Datos y Docker

### Levantar el contenedor PostgreSQL
Se debe usar siempre `--env-file config.env` para que Docker Compose resuelva variables como `${PUERTO_POSTGRES}` y `${DB_USER}`:

```bash
docker compose --env-file config.env up -d
```

### Esquemas disponibles
1. **`schema_demo.sql`**: Contiene 2 eventos de prueba (`E1` y `E2`) con 100 asientos cada uno. Utilizado para la demostración oficial.
2. **`schema.sql`**: Contiene 50 eventos con diferentes capacidades y fechas. Utilizado para pruebas de rendimiento y carga masiva.

### Reset de la Base de Datos
Para reiniciar la base de datos al estado inicial de demostración:
```bash
docker exec -i srb_postgres psql -U postgres -d srb < schema_demo.sql
```

Para verificar el estado actual de los eventos, reservas y consistencia:
```bash
./estado.sh
```
> [!NOTE]
> `estado.sh` debe ejecutarse en la máquina donde corre el contenedor de Docker (**M3**).

---

## 5. Compilación Manual

Todos los programas compilan con C++17 sin warnings con las banderas `-std=c++17 -Wall -Wextra`:

```bash
# Persistencia (M3)
g++ -std=c++17 -Wall -Wextra -O2 capa_Servicio-Almacenamiento/persistencia.cpp -o capa_Servicio-Almacenamiento/persistencia -lzmq -lpqxx -lpq -lpthread

# RMC (M2)
g++ -std=c++17 -Wall -Wextra -O2 capa_Servicios-Negocio/servicio_RMC/rmc.cpp -o capa_Servicios-Negocio/servicio_RMC/rmc -lzmq -lpthread

# Cancelación (M2)
g++ -std=c++17 -Wall -Wextra -O2 capa_Servicios-Negocio/servicio_Cancelacion/servicioCancelar.cpp -o capa_Servicios-Negocio/servicio_Cancelacion/cancelacion -lzmq -lpthread

# Gestor de Transacciones GT (M1)
g++ -std=c++17 -Wall -Wextra -O2 capa_Servicios-Negocio/servicio_GT/gestorTransacciones.cpp -o capa_Servicios-Negocio/servicio_GT/gestor -lzmq -lpthread

# Cliente
g++ -std=c++17 -Wall -Wextra -O2 capa_Consumidores/cliente.cpp -o capa_Consumidores/cliente -lzmq -lpthread
```

---

## 6. Demostración en Una Sola Máquina (`demo.sh`)

El script automatizado [demo.sh](file:///home/silvaaa/Documents/uni/5to/distri/proyecto/Reserva-De-Boletos/demo.sh) ejecuta la suite completa de demostración en `127.0.0.1`:
1. Resetea la BD con `schema_demo.sql` antes de iniciar los servicios.
2. Inicia Persistencia, Cancelación, RMC y GT con conexión segura sin contraseñas en CLI (`PGPASSWORD`).
3. **Paso B**: `cli_demo1` reserva 5 asientos de `E1`; `cli_demo2` reserva 3 asientos de `E1`.
4. **Paso C**: Consulta `E1` y verifica mediante aserción en BD (`psql -Atc`) que queda en `92/8`.
5. **Paso D**: 6 clientes concurrentes intentan reservar 20 asientos cada uno en `E2` (capacidad 100). Verifica que 5 son aceptados (`OK|`) y 1 es rechazado (`RECHAZADO`).
6. **Paso E**: Consulta `E2`, valida en BD que queda en `0/100` y verifica consistencia global (`total_ocupados == sum(asientos)`).
7. Muestra logs en vivo de cada servicio tras cada paso.

Ejecutar:
```bash
./demo.sh
```

---

## 7. Despliegue Distribuido en 3 Máquinas (`iniciar_capa.sh`)

Para la sustentación en 3 máquinas físicas o virtuales, utilice [iniciar_capa.sh](file:///home/silvaaa/Documents/uni/5to/distri/proyecto/Reserva-De-Boletos/iniciar_capa.sh), el cual toma las IPs y puertos de `config.env`:

### Orden Estricto de Arranque:

#### Paso 1: En Máquina 3 (M3 - Almacenamiento: 10.43.99.215)
1. Iniciar Docker y cargar esquema:
   ```bash
   docker compose --env-file config.env up -d
   docker exec -i srb_postgres psql -U postgres -d srb < schema_demo.sql
   ```
2. Arrancar el servicio de Persistencia:
   ```bash
   ./iniciar_capa.sh persistencia
   ```

#### Paso 2: En Máquina 2 (M2 - Negocio: 10.43.100.38)
Arrancar los servicios RMC y Cancelación en simultáneo con logs prefijados en consola:
```bash
./iniciar_capa.sh negocio
```

#### Paso 3: En Máquina 1 (M1 - Entrada: 10.43.100.37)
Arrancar el Gestor de Transacciones (GT):
```bash
./iniciar_capa.sh gt
```

#### Paso 4: En Máquina de Clientes (o M1)
Ejecutar los clientes de prueba:
```bash
./iniciar_capa.sh clientes --escenario 3
```

---

## 8. Escenarios de Carga y Concurrencia (`lanzar_clientes.sh`)

El script [lanzar_clientes.sh](file:///home/silvaaa/Documents/uni/5to/distri/proyecto/Reserva-De-Boletos/lanzar_clientes.sh) implementa los roles y escenarios de prueba de rendimiento definidos en la Tabla 1 del enunciado:

### Uso de Escenarios:
```bash
# Escenario 3 clientes:
# 2 clientes solo consulta (100% CONSULTA), 1 cliente consulta y reserva (70% CONSULTA / 30% RESERVAR)
./lanzar_clientes.sh --escenario 3 [tpm=30] [duracion=15]

# Escenario 5 clientes:
# 3 clientes solo consulta (100% CONSULTA), 2 clientes reserva y modificacion (50% RESERVAR / 50% MODIFICAR)
./lanzar_clientes.sh --escenario 5 [tpm=30] [duracion=15]

# Escenario 8 clientes:
# 4 clientes solo consulta (100% CONSULTA), 4 clientes reserva y modificacion (50% RESERVAR / 50% MODIFICAR)
./lanzar_clientes.sh --escenario 8 [tpm=30] [duracion=15]
```

### Uso Personalizado:
```bash
./lanzar_clientes.sh <N> [archivo_solicitudes|aleatorio] [tpm] [pctConsulta] [duracion]

# Ejemplos:
./lanzar_clientes.sh 5 aleatorio 40 70 20
./lanzar_clientes.sh 3 solicitudes.txt 30
```

---

## 9. Parámetros de Ejecución Directa de Binarios

- **Persistencia**:
  ```bash
  ./capa_Servicio-Almacenamiento/persistencia <puertoEscucha> "<cadenaConexionPostgres>" [trabajadores=4]
  ```
- **RMC**:
  ```bash
  ./capa_Servicios-Negocio/servicio_RMC/rmc <puertoEscucha> <endpointPersistencia> [trabajadores=4]
  ```
- **Cancelación**:
  ```bash
  ./capa_Servicios-Negocio/servicio_Cancelacion/cancelacion <puertoEscucha> <endpointPersistencia> [trabajadores=2]
  ```
- **Gestor de Transacciones**:
  ```bash
  ./capa_Servicios-Negocio/servicio_GT/gestor <puertoClientes> <endpointRmc[,endpointRmc2,...]> <endpointCancelacion> [trabajadores=4]
  ```
- **Cliente**:
  - Modo Archivo:
    ```bash
    ./capa_Consumidores/cliente <idCliente> <endpointGT> <ipCliente> <puertoNotif> <solicitudes.txt> [solicitudesPorMinuto=20]
    ```
  - Modo Aleatorio:
    ```bash
    ./capa_Consumidores/cliente <idCliente> <endpointGT> <ipCliente> <puertoNotif> aleatorio <tpm> <pctConsulta> <duracionSeg> <semilla> [pctModificar=0]
    ```

---

## 10. Empaquetado

Para generar el archivo ZIP limpio para entrega (sin binarios, objetos `.o`, logs ni directorio `.git`):
```bash
./empaquetar.sh
```
El archivo comprimido `Reserva-De-Boletos.zip` se generará en el directorio superior.