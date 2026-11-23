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
| **M2** | Capa de Negocio | `5556-5599` | TCP | Réplicas según manifiesto (`PASO_REPLICA=10`): RMC en `5556`, `5566`, `5576` (síncrono); Cancelación en `5557`, `5567` (asíncrono) |
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

# --- Manifiesto de réplicas (M2) ---
REPLICAS_RMC=3              # RMC en 5556, 5566, 5576
REPLICAS_CANCELACION=2      # Cancelación en 5557, 5567
PASO_REPLICA=10             # la réplica i escucha en PUERTO_BASE + i*PASO_REPLICA
```

> [!NOTE]
> El manifiesto es declarativo: para escalar basta con cambiar `REPLICAS_RMC` o
> `REPLICAS_CANCELACION` y reiniciar `./iniciar_capa.sh negocio` (M2) y `./iniciar_capa.sh gt` (M1).
> Ambas máquinas deben tener el mismo `config.env`.

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
2. Inicia Persistencia, **2 réplicas de Cancelación** (puertos 5557 y 5567), RMC y GT con conexión segura sin contraseñas en CLI (`PGPASSWORD`). El GT registra ambas réplicas de Cancelación.
3. **Paso B (concurrente)**: `cli_demo1` reserva 5 asientos de `E1` y `cli_demo2` reserva 3 asientos de `E1`, **al mismo tiempo**.
4. **Paso C**: Consulta `E1` y verifica mediante aserción en BD (`psql -Atc`) que queda en `92/8`.
5. **Paso D**: 6 clientes concurrentes intentan reservar 20 asientos cada uno en `E2` (capacidad 100). Verifica que 5 son aceptados (`OK|`) y 1 es rechazado (`RECHAZADO`).
6. **Paso E**: Consulta `E2`, valida en BD que queda en `0/100` y verifica consistencia global (`total_ocupados == sum(asientos)`).
7. **Paso F (cancelación asíncrona concurrente)**: 4 clientes cancelan al mismo tiempo sus reservas de 20 asientos de `E2`. Verifica que cada uno recibe su notificación `NOTIF|...|OK|`, muestra cuántas cancelaciones atendió cada réplica y comprueba en BD que `E2` queda en `80/20` y consistente.
8. Muestra logs en vivo de cada servicio tras cada paso.

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
Arranca las réplicas de RMC y Cancelación declaradas en el manifiesto (`config.env`), cada una bajo un supervisor que la reinicia si cae, con logs prefijados por réplica (`[RMC#0]`, `[CANC#1]`, ...):
```bash
./iniciar_capa.sh negocio [trabajadoresRMC=4] [trabajadoresCancelacion=2]
```
Al terminar con `Ctrl+C`, cada réplica imprime su resumen de métricas antes de cerrarse.

#### Paso 3: En Máquina 1 (M1 - Entrada: 10.43.100.37)
El GT construye la lista de réplicas desde el mismo manifiesto, las registra al arrancar (`[GT] Réplica de Cancelación registrada: ...`) y se reinicia automáticamente si el proceso cae:
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

El script [lanzar_clientes.sh](file:///home/silvaaa/Documents/uni/5to/distri/proyecto/Reserva-De-Boletos/lanzar_clientes.sh) implementa los roles y escenarios de prueba de rendimiento definidos en la Tabla 1 del enunciado.

> [!IMPORTANT]
> **Duración y Carga de Pruebas**: Para evaluar la capacidad real del sistema distribuido y estabilizar las métricas de rendimiento (promedio, desviación estándar y TPM), las pruebas de rendimiento deben durar **entre 2 y 3 minutos** (120 s a 180 s) y emplear un **TPM alto** (por defecto 300 solicitudes/minuto por cliente). Duraciones más cortas o TPMs bajos no permiten saturar los canales de concurrencia ni observar el régimen estacionario de los servicios backend.

### Uso de Escenarios:
```bash
# Escenario 3 clientes:
# 2 clientes solo consulta (100% CONSULTA), 1 cliente consulta y reserva (70% CONSULTA / 30% RESERVAR)
./lanzar_clientes.sh --escenario 3 [tpm=300] [duracion=120]

# Escenario 5 clientes:
# 3 clientes solo consulta (100% CONSULTA), 2 clientes reserva y modificacion (50% RESERVAR / 50% MODIFICAR)
./lanzar_clientes.sh --escenario 5 [tpm=300] [duracion=120]

# Escenario 8 clientes:
# 4 clientes solo consulta (100% CONSULTA), 4 clientes reserva y modificacion (50% RESERVAR / 50% MODIFICAR)
./lanzar_clientes.sh --escenario 8 [tpm=300] [duracion=120]
```

### Uso Personalizado:
```bash
./lanzar_clientes.sh <N> [archivo_solicitudes|aleatorio] [tpm=300] [pctConsulta=70] [duracion=120]

# Ejemplos:
./lanzar_clientes.sh 5 aleatorio 300 70 120
./lanzar_clientes.sh 3 solicitudes.txt 60
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
  ./capa_Servicios-Negocio/servicio_GT/gestor <puertoClientes> <endpointRmc[,endpointRmc2,...]> <endpointCancelacion[,endpointCancelacion2,...]> [trabajadores=4]
  ```
- **Cliente, modo aleatorio**:
  ```bash
  ./capa_Consumidores/cliente <idCliente> <endpointGT> <ipCliente> <puertoNotif> aleatorio <tpm> <pctConsulta> <duracionSeg> <semilla> [pctModificar=0] [pctCancelar=0]
  # o con flags: --pct-modificar <P> --pct-cancelar <P>
  ```
  `pctConsulta + pctModificar + pctCancelar` no puede superar 100; el resto son `RESERVAR`.
  `MODIFICAR` y `CANCELAR` actúan sobre la última reserva exitosa del cliente.

---

## 10. Empaquetado

Para generar el archivo ZIP limpio para entrega (sin binarios, objetos `.o`, logs ni directorio `.git`):
```bash
./empaquetar.sh
```
El archivo comprimido `Reserva-De-Boletos.zip` se generará en el directorio superior.

---

### 11. Réplicas y manifiesto

La escalabilidad de la capa de negocio se declara en `config.env` (`REPLICAS_RMC`, `REPLICAS_CANCELACION`, `PASO_REPLICA`). La réplica `i` escucha en `PUERTO_BASE + i*PASO_REPLICA`.

| Servicio | Réplicas (por defecto) | Puertos | Balanceo |
|---|---|---|---|
| RMC | 3 | 5556, 5566, 5576 | cada trabajador del GT usa un `REQ` conectado a todas las réplicas (round-robin) |
| Cancelación | 2 | 5557, 5567 | el `PUSH` del GT reparte round-robin entre los `PULL` de las réplicas |

- `./iniciar_capa.sh negocio` lanza cada réplica bajo un **supervisor** que la reinicia en 1 s si el proceso muere.
- `./iniciar_capa.sh gt` construye las listas de endpoints con el mismo manifiesto y supervisa al GT.
- Los sockets usan `ZMQ_IMMEDIATE`: solo se envía a réplicas con conexión establecida, así una réplica caída se omite sola.

---

### 12. Cancelación asíncrona

```
Cliente --REQ--> GT --ACK inmediato--> Cliente
GT --PUSH (round-robin)--> Cancelación[i] (PULL)
Cancelación[i] --REQ--> Persistencia --OK/RECHAZADO/ERROR--> Cancelación[i]
Cancelación[i] --PUSH--> PULL del cliente   (NOTIF|idReserva|OK|mensaje)
```

1. El cliente envía `CANCELAR|idPeticion|idCliente|idReserva|endpointCliente`; el GT responde `ACK` de inmediato (no bloquea) y reenvía por `PUSH` no bloqueante.
2. Una réplica de Cancelación toma el mensaje, ordena la liberación de asientos a Persistencia (`REQ/REP` con timeout y reintentos, patrón Lazy Pirate) y espera su respuesta.
3. Con ese resultado notifica **directamente** al cliente mediante un `PUSH` hacia el `PULL` que el cliente abrió (su endpoint viaja en el mensaje). Si Persistencia falla o rechaza, el cliente recibe `NOTIF|...|ERROR|motivo`.
4. Los reintentos reenvían el mismo `idPeticion`; Persistencia es **idempotente** (`reserva.req_cancelacion`), por lo que ninguna reserva se libera dos veces aunque la procese otra réplica.

**Registro de réplicas:** el GT conecta su `PUSH` a todos los endpoints del manifiesto y lo confirma en su log al arrancar (`[GT] Réplica de Cancelación registrada: tcp://...`).

---

### 13. Tolerancia a fallos y limitaciones

| Falla | Comportamiento |
|---|---|
| Cae una réplica de RMC | El supervisor la relanza en 1 s; el GT reintenta (2 intentos de 1,2 s) con el mismo `idPeticion` hacia una réplica viva. El cliente no ve el error. |
| Cae una réplica de Cancelación | El `PUSH` del GT la omite (`ZMQ_IMMEDIATE`) y el supervisor la relanza. |
| Cae el GT | El supervisor de `./iniciar_capa.sh gt` lo relanza; los clientes reintentan con Lazy Pirate y las cancelaciones pendientes en Cancelación/Persistencia no se pierden. |
| Persistencia no responde | RMC y Cancelación reintentan y devuelven `ERROR|...` al cliente (o notificación `ERROR`). |

**Limitaciones asumidas**
- Los mensajes que estaban en la cola de una réplica de Cancelación en el instante en que muere se pierden: el cliente recibió el `ACK` pero no la notificación.
- La sustitución es local a cada máquina (reinicio de procesos); no hay conmutación entre máquinas si cae un nodo completo.
- Persistencia y PostgreSQL no están replicados (único punto de falla de datos).
- Los mensajes ZeroMQ viajan sin cifrar y `idCliente` no se autentica.
