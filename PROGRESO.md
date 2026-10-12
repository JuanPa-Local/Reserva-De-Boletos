# Progreso del Proyecto SRB (Sistema de Reserva de Boletos)

Este archivo registra el avance de las tareas pendientes del SRB según los requerimientos del proyecto.
Se actualiza y se realiza commit y push tras completar cada tarea.

## Lista de Tareas

- [x] **Tarea 1**: Implementar y verificar `capa_Servicios-Negocio/servicio_RMC/rmc.cpp` (REP bind hacia GT, REQ hacia Persistencia con Lazy Pirate y retención de idPeticion, logs [INFO]/[EXITO]/[RECHAZO], métricas y resumen en SIGINT/SIGTERM).
- [x] **Tarea 2**: Corregir el bug de `idPeticion` en `cliente.cpp` (identificador de corrida: timestamp + pid para evitar colisiones al reiniciar cliente).
- [x] **Tarea 3**: Implementar el modo aleatorio en `cliente.cpp` (`./cliente <id> <endpointGT> <ip> <puertoNotif> aleatorio <tpm> <pctConsulta> <duracionSeg> <semilla>`, tpm >= 20, descubrimiento inicial con CONSULTA, 1-2 asientos en reservas, soporte de pctModificar opcional con ULTIMA).
- [x] **Tarea 4**: Implementar escenarios de la Tabla 1 en `lanzar_clientes.sh` (`--escenario 3|5|8` asignando roles a clientes).
- [x] **Tarea 5**: Correcciones a `demo.sh` (reset con schema_demo.sql antes del arranque de servicios, PGPASSWORD sin pass en conexión, conteo OK/RECHAZADO/ERROR con aserciones psql 92/8 y 0/100, logs visibles de procesos, recompilación condicional con `-nt`, uso de DB_USER).
- [x] **Tarea 6**: Configuración y documentación de `docker-compose.yml` (`docker compose --env-file config.env up -d`, verificación de carga de `schema_demo.sql`).
- [ ] **Tarea 7**: Script `iniciar_capa.sh <persistencia|negocio|gt|clientes>` para ejecución distribuida en 3 máquinas con logs visibles.
- [ ] **Tarea 8**: Documentación completa en `README.md` (dependencias, compilación, orden de arranque, puertos de red, parámetros, reset BD, demo y pruebas de carga).
- [ ] **Tarea 9**: Configurar `.gitignore` y corregir cabeceras desactualizadas en archivos `.cpp`.
