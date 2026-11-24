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
       |  - Escucha en :5556 (+ réplicas) |   |  - Escucha en :5557 (+ réplicas) |
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
                      |  - Réplica PostgreSQL de lectura (:5435) |
                      +-----------------------------+
```

### Puertos a abrir en Firewalls / Red

| Máquina | Rol | Puertos a abrir | Protocolo | Descripción |
|---|---|---|---|---|
| **M1** | Gestor de Transacciones (GT) | `5555` | TCP | Entrada de clientes a GT |
| **M2** | Capa de Negocio | `5556-5599` | TCP | Réplicas según manifiesto (`PASO_REPLICA=10`): RMC en `5556`, `5566`, `5576` (síncrono); Cancelación en `5557`, `5567` (asíncrono) |
| **M3** | Capa de Almacenamiento | `5558`, `5434`, `5435` | TCP | `5558`: Persistencia ZeroMQ<br>`5434`: PostgreSQL primaria<br>`5435`: réplica de lectura |
| **Clientes** | Clientes consumidores | `6002` en adelante (`PUERTO_NOTIF_BASE=6001` + cliente `i`) | TCP | PULL para recibir notificaciones asíncronas de cancelación |

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

El archivo [config.env](config.env) centraliza las IPs, puertos y credenciales:

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

# --- Réplica de lectura de PostgreSQL (M3) ---
PUERTO_POSTGRES_REPLICA=5435        # comenta esta línea para trabajar sin réplica
IP_POSTGRES_REPLICA=10.43.99.215    # M3, o la máquina donde pongas la réplica
REPL_PASSWORD=replica
CONTAINER_POSTGRES_REPLICA=srb_postgres_replica
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
1. **`schema_demo.sql`**: Contiene 2 eventos de prueba (`E1` y `E2`) con 100 asientos cada uno (200 asientos libres en total). Montado automáticamente por `docker-compose.yml` para la demostración funcional básica y demo de fallas.
2. **`schema.sql`**: Contiene 50 eventos (70 ocurrencias en noviembre de 2026) con capacidad de **100 asientos cada una** (7000 asientos libres en total, 0 ocupados). **Requerido para la medición de rendimiento y comparación base vs escalable**.

> [!CAUTION]
> **Alerta sobre carga de esquemas**: `docker-compose.yml` monta `schema_demo.sql` (solo 2 eventos) por defecto al inicializar el volumen. Si se olvida cargar `schema.sql` antes de medir el rendimiento, las pruebas de 3, 5 y 8 clientes correrán sobre solo 2 eventos, produciendo saturación artificial inmediata y desvirtuando los resultados.
>
> Para cargar `schema.sql` y reiniciar la base de datos a los 50 eventos:
> ```bash
> docker exec -i srb_postgres psql -U postgres -d srb < schema.sql
> ```
> Siempre verifique con `./estado.sh` que la salida reporte **70 ocurrencias, 50 eventos, 7000 asientos libres y 0 ocupados** antes de iniciar cualquier prueba de rendimiento.

### Replicación

El comando habitual levanta la base primaria y la réplica de lectura:
```bash
docker compose --env-file config.env up -d
```
En el primer arranque, la réplica se clona automáticamente desde la primaria con `pg_basebackup`; no se le carga ningún esquema. Para reiniciar ambas bases desde cero:
```bash
docker compose --env-file config.env down -v
```
Para consultar el estado y comparar los totales de la primaria y la réplica:
```bash
./estado_replica.sh
```

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

El script automatizado [demo.sh](demo.sh) ejecuta la suite completa de demostración en `127.0.0.1`:
1. Resetea la BD con `schema_demo.sql` antes de iniciar los servicios.
2. Inicia Persistencia, **2 réplicas de Cancelación** (puertos 5557 y 5567), RMC y GT con conexión segura sin contraseñas en CLI (`PGPASSWORD`). El GT registra ambas réplicas de Cancelación.
3. **Paso B (concurrente)**: `cli_demo1` reserva 5 asientos de `E1` y `cli_demo2` reserva 3 asientos de `E1`, **al mismo tiempo**.
4. **Paso C**: Consulta `E1` y verifica mediante aserción en BD (`psql -Atc`) que queda en `92/8`.
5. **Paso D**: 6 clientes concurrentes intentan reservar 20 asientos cada uno en `E2` (capacidad 100). Verifica que 5 son aceptados (`OK|`) y 1 es rechazado (`RECHAZADO`).
6. **Paso E**: Consulta `E2`, valida en BD que queda en `0/100` y verifica consistencia global (`total_ocupados == sum(asientos)`).
7. **Paso F (cancelación asíncrona concurrente)**: 4 clientes cancelan al mismo tiempo sus reservas de 20 asientos de `E2`. Verifica que cada uno recibe su notificación `NOTIF|...|OK|`, muestra cuántas cancelaciones atendió cada réplica y comprueba en BD que `E2` queda en `80/20` y consistente.
8. **Paso G**: Compara primaria y réplica (si el contenedor de la réplica existe).
9. Muestra logs en vivo de cada servicio tras cada paso.

Ejecutar:
```bash
./demo.sh
```

---

## 7. Demostración de Tolerancia a Fallos en Plena Carga (`demo_fallas.sh`)

El script automatizado [demo_fallas.sh](demo_fallas.sh) demuestra en vivo la resiliencia y alta disponibilidad del sistema ante fallos abruptos:
1. Inicia Persistencia, 2 réplicas de RMC, 2 réplicas de Cancelación y el Gestor de Transacciones (GT), cada uno protegido por un **proceso supervisor** independiente.
2. Inyecta carga concurrente con **3 clientes** enviando ráfagas continuas de `RESERVAR`, `CONSULTA` y `MODIFICAR`.
3. **Falla 1 (Muerte de RMC)**: En plena carga, envía un `kill -9` a la réplica `RMC#0`.
   - El GT detecta la desconexión y redirige de inmediato las solicitudes a la réplica `RMC#1` (o reintenta con Lazy Pirate).
   - El supervisor detecta la muerte y relanza `RMC#0` en **1 segundo**.
   - Los clientes no experimentan ningún error ni interrupción.
4. **Falla 2 (Muerte del GT)**: En plena carga, envía un `kill -9` al Gestor de Transacciones (`GT`).
   - El socket `REQ` de los clientes entra en espera bajo el patrón **Lazy Pirate** (timeout de 3000 ms, hasta 3 reintentos = **ventana total de 9 segundos**).
   - El supervisor del GT reinicia el proceso en **1 segundo**, holgadamente dentro de la ventana de 9 s de los clientes.
   - **Continuar donde se quedó**: Los clientes reconectan y reenvían la solicitud con el **mismo `idPeticion`**.
   - En Persistencia, la restricción `UNIQUE` sobre `reserva.req_id` garantiza **idempotencia**: si la reserva ya se había insertado antes de la caída del GT, se detecta el duplicado y se devuelve el id existente; si no, se ejecuta limpiamente. Ninguna reserva se duplica ni se pierde.
5. **Auditoría final**: Comprueba que los clientes completaron todas sus solicitudes con **0 errores de desconexión/timeout** y verifica en PostgreSQL que la consistencia global es perfecta (`total_ocupados == sum(asientos_activos)` y 0 duplicados por `req_id`).

Ejecutar:
```bash
./demo_fallas.sh
```

---

## 8. Comparación: Arquitectura Base vs Arquitectura Escalable

Para realizar una comparación justa y científicamente válida del rendimiento entre la **Arquitectura Base** y la **Arquitectura Escalable**, se define con rigor el número de procesos y trabajadores por capa:

### Definición de Trabajadores y Recursos

| Componente | Arquitectura Base (Mínima) | Arquitectura Escalable | Justificación / Rol |
|---|---|---|---|
| **GT (Gestor)** | 1 proceso, **4 trabajadores** | 1 proceso, **4 trabajadores** | Enruta solicitudes síncronas a RMC y asíncronas a Cancelación |
| **RMC** | 1 réplica, **4 trabajadores** (4 hilos totales) | 3 réplicas × 4 trabajadores = **12 trabajadores totales** | Procesa reservas, consultas y modificaciones concurrentes |
| **Cancelación** | 1 réplica, **2 trabajadores** (2 hilos totales) | 2 réplicas × 2 trabajadores = **4 trabajadores totales** | Procesa liberaciones asíncronas y notifica por PULL |
| **Persistencia** | 1 proceso, **4 trabajadores** (solo primaria) | 1 proceso, **4 trabajadores** (pool hacia primaria y réplica) | Pool de conexiones `libpqxx` a PostgreSQL |
| **PostgreSQL** | 1 instancia primaria (puerto `5434`) | 1 primaria (`5434`) + 1 réplica streaming (`5435`) | Primaria atiende escrituras; réplica atiende consultas |

### Cómo correr la Arquitectura Base

1. **Configurar el manifiesto en `config.env`**:
   ```env
   REPLICAS_RMC=1
   REPLICAS_CANCELACION=1
   # PUERTO_POSTGRES_REPLICA=5435     <-- Comentar esta línea para desactivar la réplica de lectura
   ```
2. **Cargar `schema.sql` y resetear la BD**:
   ```bash
   docker exec -i srb_postgres psql -U postgres -d srb < schema.sql
   ./estado.sh   # Validar: 70 ocurrencias, 7000 libres, 0 ocupados
   ```
3. **Iniciar las capas**:
   - En M3 (Almacenamiento): `./iniciar_capa.sh persistencia 4`
   - En M2 (Negocio): `./iniciar_capa.sh negocio 4 2`
   - En M1 (Entrada/GT): `./iniciar_capa.sh gt 4`
4. **Ejecutar pruebas de carga**:
   - En Clientes: `./lanzar_clientes.sh --escenario 3 300 120`

### Cómo correr la Arquitectura Escalable

1. **Configurar el manifiesto en `config.env`**:
   ```env
   REPLICAS_RMC=3
   REPLICAS_CANCELACION=2
   PUERTO_POSTGRES_REPLICA=5435       <-- Activa la réplica de lectura de PostgreSQL
   ```
2. **Cargar `schema.sql` y resetear la BD**:
   ```bash
   docker exec -i srb_postgres psql -U postgres -d srb < schema.sql
   sleep 2        # Permite sincronizar el streaming WAL a la réplica
   ./estado_replica.sh
   ```
3. **Iniciar las capas**:
   - En M3 (Almacenamiento): `./iniciar_capa.sh persistencia 4`
   - En M2 (Negocio): `./iniciar_capa.sh negocio 4 2`
   - En M1 (Entrada/GT): `./iniciar_capa.sh gt 4`
4. **Ejecutar pruebas de carga**:
   - En Clientes: `./lanzar_clientes.sh --escenario 3 300 120`

> [!IMPORTANT]
> **Reseteo obligatorio de la Base de Datos**: `docker-compose.yml` monta `schema_demo.sql` (solo 2 eventos) por defecto. Si olvida cargar `schema.sql` antes de medir, las pruebas correrán sobre 2 eventos provocando colisiones artificiales que invalidan los resultados. **Siempre reinicie la BD con `schema.sql` entre cada corrida de prueba** para asegurar condiciones iniciales idénticas (7000 asientos disponibles).

---

## 9. Despliegue Distribuido en 3 Máquinas (`iniciar_capa.sh`)

Para la sustentación en 3 máquinas físicas o virtuales, utilice [iniciar_capa.sh](iniciar_capa.sh), el cual toma las IPs y puertos de `config.env`:

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

## 10. Escenarios de Carga y Concurrencia (`lanzar_clientes.sh`)

El script [lanzar_clientes.sh](lanzar_clientes.sh) implementa los roles y escenarios de prueba de rendimiento definidos en la Tabla 1 del enunciado.

> [!IMPORTANT]
> **Duración y Carga de Pruebas**: Para evaluar la capacidad real del sistema distribuido y estabilizar las métricas de rendimiento (promedio, desviación estándar y TPM), las pruebas de rendimiento deben durar **entre 2 y 3 minutos** (120 s a 180 s) y emplear un **TPM alto** (por defecto 300 solicitudes/minuto por cliente). Duraciones más cortas o TPMs bajos no permiten saturar los canales de concurrencia ni observar el régimen estacionario de los servicios backend.

### Parámetro `--mes` (Genérico para cualquier mes de prueba):
Permite especificar el mes a consultar en modo aleatorio (del 1 al 12). Si el profesor evalúa con un caso de prueba para otro mes (por ejemplo, octubre o diciembre), se pasa `--mes <MM>` (o variable de entorno `MES=<MM>`); de lo contrario, por defecto consulta el mes `11`.
```bash
./lanzar_clientes.sh --escenario 3 300 120 --mes 11
./lanzar_clientes.sh 5 aleatorio 300 70 120 --mes 12
```

### Uso de Escenarios:
```bash
# Escenario 3 clientes:
# 2 clientes solo consulta (100% CONSULTA), 1 cliente consulta y reserva (70% CONSULTA / 30% RESERVAR)
./lanzar_clientes.sh --escenario 3 [tpm=300] [duracion=120] [--mes 11]

# Escenario 5 clientes:
# 3 clientes solo consulta (100% CONSULTA), 2 clientes reserva y modificacion (50% RESERVAR / 50% MODIFICAR)
./lanzar_clientes.sh --escenario 5 [tpm=300] [duracion=120] [--mes 11]

# Escenario 8 clientes:
# 4 clientes solo consulta (100% CONSULTA), 4 clientes reserva y modificacion (50% RESERVAR / 50% MODIFICAR)
./lanzar_clientes.sh --escenario 8 [tpm=300] [duracion=120] [--mes 11]
```

### Uso Personalizado:
```bash
./lanzar_clientes.sh <N> [archivo_solicitudes|aleatorio] [tpm=300] [pctConsulta=70] [duracion=120] [--mes 11]

# Ejemplos:
./lanzar_clientes.sh 5 aleatorio 300 70 120 --mes 11
./lanzar_clientes.sh 3 solicitudes.txt 60
```

---

## 11. Parámetros de Ejecución Directa de Binarios

- **Persistencia**:
  ```bash
  ./capa_Servicio-Almacenamiento/persistencia <puertoEscucha> "<cadenaPrimaria>" [trabajadores=4] ["<cadenaReplica1>;<cadenaReplica2>;..."]
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
  ./capa_Consumidores/cliente <idCliente> <endpointGT> <ipCliente> <puertoNotif> aleatorio <tpm> <pctConsulta> <duracionSeg> <semilla> [pctModificar=0] [pctCancelar=0] [--mes <MM>]
  # o con flags: --pct-modificar <P> --pct-cancelar <P> --mes <MM>
  ```
  `pctConsulta + pctModificar + pctCancelar` no puede superar 100; el resto son `RESERVAR`.
  `MODIFICAR` y `CANCELAR` actúan sobre la última reserva exitosa del cliente.
  `--mes` permite especificar el mes (1-12) para que las consultas y el descubrimiento automático de eventos consulten el mes deseado (por defecto 11).

---

## 12. Empaquetado

Para generar el archivo ZIP limpio para entrega (sin binarios, objetos `.o`, logs ni directorio `.git`):
```bash
./empaquetar.sh
```
El archivo comprimido `Reserva-De-Boletos.zip` se generará en el directorio superior.

---

### 13. Réplicas y manifiesto

La escalabilidad de la capa de negocio se declara en `config.env` (`REPLICAS_RMC`, `REPLICAS_CANCELACION`, `PASO_REPLICA`). La réplica `i` escucha en `PUERTO_BASE + i*PASO_REPLICA`.

| Servicio | Réplicas (por defecto) | Puertos | Balanceo |
|---|---|---|---|
| RMC | 3 | 5556, 5566, 5576 | cada trabajador del GT usa un `REQ` conectado a todas las réplicas (round-robin) |
| Cancelación | 2 | 5557, 5567 | el `PUSH` del GT reparte round-robin entre los `PULL` de las réplicas |

- `./iniciar_capa.sh negocio` lanza cada réplica bajo un **supervisor** que la reinicia en 1 s si el proceso muere.
- `./iniciar_capa.sh gt` construye las listas de endpoints con el mismo manifiesto y supervisa al GT.
- `./iniciar_capa.sh persistencia` supervisa al servicio de persistencia y lo reinicia en 1 s ante caídas anómalas.
- Los sockets usan `ZMQ_IMMEDIATE`: solo se envía a réplicas con conexión establecida, así una réplica caída se omite sola.

---

### 14. Cancelación asíncrona

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

### 15. Tolerancia a fallos y limitaciones

| Falla | Comportamiento |
|---|---|
| Cae una réplica de RMC | El supervisor la relanza en 1 s; el GT reintenta (2 intentos de 1,2 s) con el mismo `idPeticion` hacia una réplica viva. El cliente no ve el error. |
| Cae una réplica de Cancelación | El `PUSH` del GT la omite (`ZMQ_IMMEDIATE`) y el supervisor la relanza. |
| Cae el GT | El supervisor de `./iniciar_capa.sh gt` lo relanza y los clientes reintentan con Lazy Pirate. |
| Cae la réplica de lectura | Las `CONSULTA` pasan a la primaria y la réplica se reintenta cada 5 s; las escrituras no se afectan. |
| Cae la primaria | Las escrituras devuelven `ERROR`. Recuperación manual: `docker exec srb_postgres_replica psql -U postgres -c "SELECT pg_promote();"`, luego poner en `config.env` el host y puerto de la réplica como primaria y reiniciar Persistencia. |
| Persistencia no responde | RMC y Cancelación reintentan y devuelven `ERROR|...` al cliente (o notificación `ERROR`). |

**Limitaciones asumidas**
- Los mensajes que estaban en la cola de una réplica de Cancelación en el instante en que muere se pierden: el cliente recibió el `ACK` pero no la notificación.
- La sustitución es local a cada máquina (reinicio de procesos); no hay conmutación entre máquinas si cae un nodo completo.
- La réplica de PostgreSQL es solo de lectura; si cae la primaria, la promoción y el cambio de configuración requieren intervención manual.
- Los mensajes ZeroMQ viajan sin cifrar y `idCliente` no se autentica.

---

## 16. Análisis Crítico de Escalabilidad y Riesgos de Rendimiento

El enunciado de la práctica plantea la pregunta central: **"¿Cuánto realmente mejoran las métricas al escalar la arquitectura?"**. En un sistema distribuido real, un análisis riguroso de los cuellos de botella y las restricciones físicas vale más que resultados inflados o artificiales:

### Cuellos de Botella Naturales y Restricciones Físicas

1. **Cuello de botella en la capa de Almacenamiento (Persistencia)**:
   - Se cuenta con **3 réplicas de RMC** y **2 de Cancelación** en la capa de negocio, pero solo **1 instancia del servicio de Persistencia** (con 4 trabajadores) atendiendo sus peticiones.
   - Aunque la capa de negocio escala horizontalmente, todas las operaciones de escritura (`RESERVAR`, `MODIFICAR`, `LIBERAR`) convergen en la misma base de datos PostgreSQL primaria, donde se aplican bloqueos pesimistas a nivel de fila (`SELECT ... FOR UPDATE`) para garantizar la no sobreventa. Por ende, la concurrencia máxima de escritura está acotada por el motor relacional de la primaria.

2. **Co-ubicación de réplicas en hardware compartido**:
   - En el entorno de pruebas, la réplica de PostgreSQL corre en la misma máquina física/VM que la primaria (`IP_POSTGRES_REPLICA = IP_PERSISTENCIA`), y las 3 réplicas de RMC comparten los núcleos y la memoria de M2.
   - Al compartir procesador y canal de entrada/salida (I/O) a disco, la ganancia de rendimiento en consultas puede ser modesta si el cuello de botella es la CPU o el disco local.

3. **Carga Ofrecida vs. Throughput Medido (Saturación)**:
   - Con un ritmo regulado de peticiones (p. ej. `TPM=300` por cliente, equivalente a 1 solicitud cada 200 ms), si la arquitectura base **no está saturada**, el TPM medido será exactamente igual al TPM ofrecido, y la latencia promedio será baja en ambos casos.
   - Para observar una diferencia estadística real entre la arquitectura base y la escalable, **es necesario someter la arquitectura base a una carga que la sature** (ej. 5 a 8 clientes concurrentes con 70% a 100% de consultas masivas).

4. **Verificación empírica de la descarga de lectura**:
   - Para demostrar que la réplica de lectura está operando y descargando a la primaria, el servicio de Persistencia registra un contador atómico de lecturas que se imprime al finalizar la ejecución:
     ```text
     [PERSISTENCIA] CONSULTA servidas por réplicas=X  por la primaria=Y
     ```
   - Además, se puede constatar la distribución de carga midiendo el uso de CPU con herramientas del sistema operativo (`top` o `pidstat -p <PIDs>`).

5. **Consistencia Eventual y Replication Lag**:
   - Las consultas se atienden desde la réplica de lectura para liberar a la primaria. La réplica de PostgreSQL se actualiza de manera **asíncrona** por streaming de WAL (*Write-Ahead Logging*).
   - Existe una ventana de replicación (*replay lag*) típicamente de **2 a 5 milisegundos** (verificable con `./estado_replica.sh`).
   - **Efecto de diseño**: Si un cliente realiza una consulta inmediatamente después de confirmar una reserva (en menos de 2-3 ms), existe una probabilidad teórica de que la réplica no haya aplicado aún el registro WAL y la consulta muestre momentáneamente los asientos anteriores. Este compromiso de consistencia eventual a cambio de escalabilidad masiva en lectura es inherente a arquitecturas basadas en líder-seguidor (CQRS).

---

## 17. Preguntas Frecuentes y Justificaciones para Sustentación / Video

Esta sección recopila las justificaciones arquitectónicas y teóricas ante las preguntas clave del equipo docente en la sustentación y entrega del proyecto:

### 1. Topología de Red: ¿Por qué 4 máquinas si el enunciado pide "al menos 3"?
- **Pregunta**: El enunciado establece "al menos 3 máquinas con cada capa en una máquina distinta". ¿Cómo se justifica la topología adoptada?
- **Justificación**: Se implementó una topología distribuida estricta de 4 máquinas (o 4 roles de red aislados):
  - **M1**: Gestor de Transacciones (Capa de Entrada y Enrutamiento).
  - **M2**: Capa de Negocio (Réplicas de RMC y Cancelación).
  - **M3**: Capa de Almacenamiento (Persistencia y contenedores PostgreSQL primaria/réplica).
  - **M4**: Clientes consumidores (Generación de carga).
- Cumple el requisito de "al menos 3" ($4 \ge 3$) y "cada capa en una máquina". Colocar los clientes en una máquina dedicada (M4) es una buena práctica en sistemas distribuidos para evitar que el generador de carga consuma los ciclos de CPU y memoria de los servidores bajo prueba (interferencia de co-ubicación).

### 2. Supervisión del Servicio de Almacenamiento (Persistencia)
- **Pregunta**: El enunciado exigía supervisión y reinicio para GT y RMC. ¿Por qué supervisar también Persistencia?
- **Justificación**: Persistencia es un servicio sin estado a nivel de proceso (el estado reside en PostgreSQL). Para evitar que Persistencia sea un punto único de falla (SPOF) por caídas de software, `iniciar_capa.sh persistencia` incluye un proceso supervisor que detecta su terminación y lo relanza automáticamente en 1 segundo, reconectando el pool de conexiones `libpqxx` sin perder datos ni estado transaccional.

### 3. Caída de una réplica de Cancelación y pérdida de mensajes en tránsito
- **Pregunta**: Si una réplica de Cancelación recibe un `kill -9`, ¿qué sucede con las solicitudes en cola?
- **Justificación**: El protocolo de cancelación es asíncrono: el cliente envía la solicitud al GT, recibe un `ACK` inmediato y el GT la enruta por un socket ZeroMQ `PUSH` hacia el `PULL` de las réplicas de Cancelación. Si una réplica muere abruptamente mientras tiene mensajes encolados en su memoria RAM (antes de haber emitido el comando a Persistencia), esos mensajes en memoria se pierden. El cliente recibió el `ACK` del GT pero nunca recibirá la notificación final. Esta es una **limitación técnica asumida y documentada con honestidad**: implementar entrega 100% garantizada ante fallos de proceso en colas exigiría un broker con persistencia en disco (como RabbitMQ o Apache Kafka con `fsync`), lo cual excede el alcance de ZeroMQ en memoria.

### 4. Cálculo de Métricas: Desviación Estándar y Latencia Extremo a Extremo
- **Pregunta**: ¿Cómo se calculan la desviación estándar y la latencia reportadas en las tablas?
- **Justificación**:
  - **Desviación Estándar Poblacional**: El cálculo implementado en `Estadisticas::imprimirResumen()` utiliza la desviación estándar poblacional dividiendo por $N$ ($\sigma = \sqrt{\frac{1}{N}\sum(x_i - \mu)^2}$).
  - **Latencia Extremo a Extremo vs. Interna**: El Gestor de Transacciones (GT) mide la latencia interna del backend (desde que extrae el mensaje de su socket ROUTER hasta que responde). En contraste, el **Cliente mide la latencia extremo a extremo (Round-Trip Time o RTT)**, que abarca el viaje completo por la red física, la serialización/deserialización, el encolamiento, el procesamiento en capas y los reintentos de Lazy Pirate en caso de fallas transitorias. Las tablas del informe reportan la métrica del cliente por reflejar la experiencia real percibida por el usuario.

### 5. Mapeo a las Estrategias Vistas en Clase de Sistemas Distribuidos
- El enunciado exige contrastar cada decisión con las estrategias teóricas del curso:
  1. **Réplicas Activas-Activas (Redundancia horizontal)**: 3 réplicas de RMC y 2 de Cancelación atendiendo carga en paralelo en la capa de negocio.
  2. **Réplicas Líder-Seguidor (Active-Passive con Read-Only Replica)**: PostgreSQL primaria para escrituras atómicas y PostgreSQL réplica con replicación por streaming físico de WAL para escalar lecturas masivas.
  3. **Balanceo de Carga**: Round-robin sin estado implementado en ZeroMQ. Para RMC, los hilos del GT balancean sus sockets `REQ` entre todas las réplicas; para Cancelación, el socket `PUSH` reparte round-robin entre los `PULL` de las réplicas disponibles (`ZMQ_IMMEDIATE`).
  4. **Supervisión de Procesos (Watchdog)**: Patrón supervisor inspirado en Erlang/OTP implementado en los scripts bash de arranque (`iniciar_capa.sh`, `demo_fallas.sh`), que detecta la muerte anómala de cualquier hijo y lo reinicia en 1 segundo.
  5. **Detección de Fallos y Reintentos**: Patrón Lazy Pirate (timeout de socket + reintento transparente con socket limpio) implementado en el Cliente hacia el GT, y en el RMC hacia Persistencia.
  6. **Idempotencia y Semántica Exactly-Once**: El reintento con el mismo `idPeticion` garantiza semántica *At-Least-Once* en el transporte de red, que se convierte en *Exactly-Once* a nivel de negocio en PostgreSQL mediante la restricción `UNIQUE` en `reserva.req_id` y el registro de `req_cancelacion`.
