#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
probar_fallas.py - Prueba automática de tolerancia a fallas del SRB (segunda entrega)

Qué demuestra
-------------
El enunciado exige que la caída del Gestor de Transacciones (GT) o del RMC sea TRANSPARENTE para el cliente:
un nuevo proceso sustituye al caído y las operaciones continúan donde se habían dejado.
Este script lo comprueba con evidencia:

  1. Lanza N clientes con carga mixta (consulta, reserva, modificación y cancelación) durante --duracion s.
  2. Mientras hay carga, mata con `kill -9` (sin avisar, como una falla real):
        - una réplica de RMC                         (al 15 % del tiempo)
        - el Gestor de Transacciones (GT)            (al 35 %)
        - TODAS las réplicas de RMC a la vez         (al 55 %)
        - una réplica de Cancelación (opcional, --incluir-cancelacion)  (al 75 %)
     y mide cuánto tarda el supervisor (iniciar_capa.sh) en reponer cada proceso.
  3. Al terminar verifica, con los logs de los clientes y la BD:
        - TRANSPARENCIA : 0 respuestas ERROR en consulta/reserva/modificación
        - RECUPERACIÓN  : todos los procesos repuestos dentro de la tolerancia
        - CONSISTENCIA  : asientos ocupados == suma de asientos de reservas activas
        - EXACTAMENTE UNA VEZ: filas en `reserva` == reservas OK vistas por los clientes
          (ni duplicadas por los reintentos, ni perdidas)
        - NOTIFICACIONES: ACK de cancelación == notificaciones recibidas (WARN si se mató Cancelación:
          es la limitación documentada en el README)

Requisitos
----------
- El sistema ya está levantado (./iniciar_capa.sh persistencia | negocio | gt) con supervisores.
- Ejecútalo en la máquina de los CLIENTES. Los procesos a matar viven en M1 (GT) y M2 (negocio):
    * si ejecutas todo en una sola máquina (demo local) no necesitas nada más;
    * si están en máquinas distintas: --ssh-gt usuario@IP_GT  --ssh-negocio usuario@IP_RMC
      (ssh por llave, sin contraseña: `ssh-copy-id usuario@host`).
- Reseteo/verificación de la BD: por defecto docker exec local o psql; si la BD está en otra máquina usa
  --reset-cmd "ssh usuario@M3 'docker exec -i srb_postgres psql -q -U postgres -d srb'" (recibe SQL por stdin).

Uso
---
    ./probar_fallas.py --simulacro                      # solo muestra qué procesos mataría (ensayo seguro)
    ./probar_fallas.py                                  # prueba completa de 90 s
    ./probar_fallas.py --duracion 120 --clientes 5 --incluir-cancelacion \\
        --ssh-gt grupo@10.43.100.37 --ssh-negocio grupo@10.43.100.38

Termina con código 0 si todas las verificaciones críticas pasan y 1 si alguna falla.
Solo usa la librería estándar de Python 3.
"""

import argparse
import collections
import datetime
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import time

RE_SOLICITUD = re.compile(r"^(CONSULTA|RESERVAR|MODIFICAR|CANCELAR)\|.*?  ->  (.*)  \(([0-9.]+) ms\)\s*$")
RE_NOTIF = re.compile(r"\[NOTIFICACION\] NOTIF\|[^|]*\|(OK|ERROR)\|")
RE_ENTERO = re.compile(r"^\s*-?\d+\s*$")
SINCRONAS = ("CONSULTA", "RESERVAR", "MODIFICAR")

USA_COLOR = sys.stdout.isatty()


def color(texto, codigo):
    return f"\033[{codigo}m{texto}\033[0m" if USA_COLOR else texto


def verde(t): return color(t, "32")
def rojo(t): return color(t, "31")
def amarillo(t): return color(t, "33")
def negrita(t): return color(t, "1")


# ----------------------------------------------------------------------------- utilidades

def leer_config(proyecto):
    cfg = {}
    ruta = os.path.join(proyecto, "config.env")
    if os.path.isfile(ruta):
        with open(ruta, encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if not linea or linea.startswith("#") or "=" not in linea:
                    continue
                clave, valor = linea.split("=", 1)
                cfg[clave.strip()] = re.split(r"\s+#", valor, maxsplit=1)[0].strip().strip('"').strip("'")
    return cfg


class Destino:
    """Máquina donde se ejecutan comandos: local o por ssh."""

    def __init__(self, nombre, ssh=None):
        self.nombre = nombre
        self.ssh = ssh

    def ejecutar(self, comando, timeout=20):
        if self.ssh:
            args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", self.ssh, comando]
            r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        else:
            r = subprocess.run(comando, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()

    def descripcion(self):
        return self.ssh if self.ssh else "esta máquina"


class Proceso:
    """Un proceso del SRB que se puede localizar y matar."""

    def __init__(self, nombre, ruta_relativa, puerto, destino):
        self.nombre = nombre
        self.destino = destino
        self.puerto = puerto
        # El truco [x]resto evita que pgrep -f se encuentre a sí mismo (ni al shell remoto de ssh)
        self.patron = f"[{ruta_relativa[0]}]{ruta_relativa[1:]} {puerto} "

    def pids(self):
        rc, salida, _ = self.destino.ejecutar(f"pgrep -f {shlex.quote(self.patron)}")
        return sorted(int(p) for p in salida.split() if p.isdigit())

    def matar(self, pids):
        if pids:
            self.destino.ejecutar("kill -9 " + " ".join(str(p) for p in pids))


def comando_sql(cfg, reset_cmd):
    env = dict(os.environ)
    env["PGPASSWORD"] = cfg.get("DB_PASSWORD", "postgres")
    if reset_cmd:
        return reset_cmd, env
    usuario = cfg.get("DB_USER", "postgres")
    base = cfg.get("DB_NAME", "srb")
    contenedor = cfg.get("CONTAINER_POSTGRES", "srb_postgres")
    if shutil.which("docker"):
        r = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
        if r.returncode == 0 and contenedor in r.stdout.split():
            return f"docker exec -i {contenedor} psql -q -U {usuario} -d {base} -v ON_ERROR_STOP=1", env
    if shutil.which("psql"):
        return (f"psql -q -h {cfg.get('IP_PERSISTENCIA', '127.0.0.1')} -p {cfg.get('PUERTO_POSTGRES', '5434')} "
                f"-U {usuario} -d {base} -v ON_ERROR_STOP=1"), env
    return None, None


def ejecutar_sql(cmd, env, sql):
    return subprocess.run(cmd, shell=True, input=sql, capture_output=True, text=True, env=env, timeout=120)


def entero_sql(cmd, env, sql):
    r = ejecutar_sql(cmd, env, sql)
    if r.returncode != 0:
        return None
    for linea in r.stdout.splitlines():
        if RE_ENTERO.match(linea):
            return int(linea)
    return None


SQL_RESERVAS = "SELECT count(*) FROM reserva;"
SQL_CANCELADAS = "SELECT count(*) FROM reserva WHERE estado='CANCELADA';"
SQL_CONSISTENTE = (
    "SELECT CASE WHEN (SELECT COALESCE(sum(ocupados),0) FROM evento) = "
    "(SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA') THEN 1 ELSE 0 END;"
)


# ----------------------------------------------------------------------------- análisis de logs

def analizar_logs(rutas):
    """Resume los logs de los clientes: conteos por tipo/resultado, latencias, notificaciones."""
    por_tipo = {t: collections.Counter() for t in ("CONSULTA", "RESERVAR", "MODIFICAR", "CANCELAR")}
    latencias = []
    errores = collections.Counter()
    notif = collections.Counter()
    for ruta in rutas:
        if not os.path.isfile(ruta):
            continue
        with open(ruta, encoding="utf-8", errors="replace") as f:
            for linea in f:
                m = RE_SOLICITUD.match(linea)
                if m:
                    tipo, resp, ms = m.group(1), m.group(2), float(m.group(3))
                    if resp.startswith("ERROR"):
                        por_tipo[tipo]["ERROR"] += 1
                        errores[f"{tipo}: {resp[:70]}"] += 1
                    elif resp.startswith("OK|"):
                        por_tipo[tipo]["OK"] += 1
                        latencias.append(ms)
                    elif resp.startswith("ACK|"):
                        por_tipo[tipo]["ACK"] += 1
                        latencias.append(ms)
                    else:
                        por_tipo[tipo]["RECHAZADO"] += 1
                        latencias.append(ms)
                    continue
                m = RE_NOTIF.search(linea)
                if m:
                    notif[m.group(1)] += 1
    return {"por_tipo": por_tipo, "latencias": latencias, "errores": errores, "notif": notif}


def percentil(valores, p):
    if not valores:
        return None
    orden = sorted(valores)
    return orden[min(len(orden) - 1, max(0, math.ceil(p * len(orden)) - 1))]


def resumen_en_vivo(rutas):
    total = errores = 0
    for ruta in rutas:
        try:
            with open(ruta, encoding="utf-8", errors="replace") as f:
                for linea in f:
                    if "  ->  " in linea:
                        total += 1
                        if "  ->  ERROR" in linea:
                            errores += 1
        except OSError:
            pass
    return total, errores


# ----------------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Prueba de tolerancia a fallas del SRB (kill -9 con carga)")
    ap.add_argument("--proyecto", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--duracion", type=int, default=90, help="segundos de carga (por defecto 90)")
    ap.add_argument("--tpm", type=int, default=120, help="solicitudes por minuto por cliente (mínimo 20)")
    ap.add_argument("--clientes", type=int, default=4)
    ap.add_argument("--mix", default="40,20,20",
                    help="%% consulta,%% modificación,%% cancelación; el resto son reservas (defecto 40,20,20)")
    ap.add_argument("--ssh-gt", help="usuario@host de la máquina del GT (si no es esta)")
    ap.add_argument("--ssh-negocio", help="usuario@host de la máquina de RMC/Cancelación (si no es esta)")
    ap.add_argument("--incluir-cancelacion", action="store_true", help="también mata una réplica de Cancelación")
    ap.add_argument("--max-recuperacion", type=float, default=15.0,
                    help="segundos máximos para considerar repuesto un proceso (defecto 15)")
    ap.add_argument("--reset-cmd", help="comando que recibe SQL por stdin para resetear/consultar la BD")
    ap.add_argument("--schema", help="esquema SQL para el reset (por defecto <proyecto>/schema.sql)")
    ap.add_argument("--sin-reset", action="store_true", help="no resetear la BD (la verificación exactly-once perderá validez)")
    ap.add_argument("--salida", default="resultados_fallas")
    ap.add_argument("--simulacro", action="store_true", help="solo lista los procesos que se matarían y termina")
    ap.add_argument("--si", action="store_true", help="no pedir confirmación")
    args = ap.parse_args()

    proyecto = os.path.abspath(args.proyecto)
    cfg = leer_config(proyecto)
    if args.tpm < 20:
        sys.exit("--tpm debe ser al menos 20 (mínimo del enunciado).")
    try:
        pct_cons, pct_mod, pct_canc = (int(x) for x in args.mix.split(","))
    except ValueError:
        sys.exit("--mix debe ser 'consulta,modificacion,cancelacion', p. ej. 40,20,20")
    if pct_cons + pct_mod + pct_canc > 100:
        sys.exit("--mix suma más de 100")

    ip_gt = cfg.get("IP_GT", "127.0.0.1")
    puerto_gt = int(cfg.get("PUERTO_GT", "5555"))
    paso = int(cfg.get("PASO_REPLICA", "10"))
    n_rmc = int(cfg.get("REPLICAS_RMC", "1"))
    n_canc = int(cfg.get("REPLICAS_CANCELACION", "1"))
    puerto_rmc = int(cfg.get("PUERTO_RMC", "5556"))
    puerto_canc = int(cfg.get("PUERTO_CANCELACION", "5557"))

    destino_gt = Destino("GT", args.ssh_gt)
    destino_neg = Destino("negocio", args.ssh_negocio)
    gt = Proceso("GT", "servicio_GT/gestor", puerto_gt, destino_gt)
    rmcs = [Proceso(f"RMC#{i}", "servicio_RMC/rmc", puerto_rmc + i * paso, destino_neg) for i in range(n_rmc)]
    cancs = [Proceso(f"CANC#{i}", "servicio_Cancelacion/cancelacion", puerto_canc + i * paso, destino_neg)
             for i in range(n_canc)]

    # --- verificación previa: ¿están todos los procesos?
    print(negrita("=" * 78))
    print(negrita(" PRUEBA DE TOLERANCIA A FALLAS DEL SRB"))
    print(negrita("=" * 78))
    print(f" GT en {destino_gt.descripcion()} (puerto {puerto_gt}) | RMC x{n_rmc} y Cancelación x{n_canc} en "
          f"{destino_neg.descripcion()}")
    faltan = []
    for p in [gt] + rmcs + cancs:
        try:
            ids = p.pids()
        except Exception as e:  # ssh caído, etc.
            sys.exit(f"No pude consultar procesos en {p.destino.descripcion()}: {e}")
        estado = verde(f"PID {','.join(map(str, ids))}") if ids else rojo("NO ENCONTRADO")
        print(f"   {p.nombre:<7} puerto {p.puerto:<5} {estado}")
        if not ids:
            faltan.append(p.nombre)
    if faltan:
        sys.exit(rojo(f"\nNo encuentro: {', '.join(faltan)}. Levanta el sistema con ./iniciar_capa.sh "
                      f"(los supervisores son los que reponen los procesos) o usa --ssh-gt/--ssh-negocio si "
                      f"viven en otras máquinas."))
    if args.simulacro:
        print("\n[simulacro] Todo localizado. No se mató nada.")
        return

    # --- plan de fallas
    D = args.duracion
    eventos = []
    if n_rmc >= 2:
        eventos.append((0.15, "Caída de UNA réplica de RMC", [rmcs[min(1, n_rmc - 1)]]))
    eventos.append((0.35, "Caída del Gestor de Transacciones (GT)", [gt]))
    eventos.append((0.55, "Caída simultánea de TODAS las réplicas de RMC" if n_rmc > 1 else "Caída del RMC", rmcs))
    if args.incluir_cancelacion:
        eventos.append((0.75, "Caída de una réplica de Cancelación", [cancs[0]]))
    print("\n Plan de fallas (kill -9) sobre", D, "s de carga:")
    for frac, nombre, objetivos in eventos:
        print(f"   t={frac * D:>5.1f} s  {nombre}  [{', '.join(o.nombre for o in objetivos)}]")
    if not args.si:
        input("\n Pulsa Enter para comenzar (Ctrl+C para cancelar)... ")

    # --- BD
    cmd_sql, env_sql = comando_sql(cfg, args.reset_cmd)
    schema = os.path.abspath(args.schema) if args.schema else os.path.join(proyecto, "schema.sql")
    if not args.sin_reset:
        if cmd_sql is None or not os.path.isfile(schema):
            sys.exit("No puedo resetear la BD (sin docker/psql o sin schema.sql). Usa --reset-cmd o --sin-reset.")
        with open(schema, encoding="utf-8") as f:
            r = ejecutar_sql(cmd_sql, env_sql, f.read())
        if r.returncode != 0:
            sys.exit(f"Falló el reset de la BD:\n{r.stderr}")
        print("\n[reset] BD restaurada al estado inicial")
        time.sleep(2)
    if cmd_sql is None:
        print(amarillo("[aviso] sin acceso a la BD: se omiten las verificaciones de consistencia y exactly-once"))

    # --- carpeta de resultados y binario del cliente
    sello = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    carpeta = os.path.join(os.path.abspath(args.salida), sello)
    os.makedirs(carpeta)
    binario = os.path.join(proyecto, "capa_Consumidores", "cliente")
    fuente = os.path.join(proyecto, "capa_Consumidores", "cliente.cpp")
    if not os.path.isfile(binario) or os.path.getmtime(fuente) > os.path.getmtime(binario):
        print("[-] Compilando cliente...")
        subprocess.run(["g++", "-std=c++17", "-O2", fuente, "-o", binario, "-lzmq", "-lpthread"], check=True)

    # --- lanzar clientes
    ip_clientes = cfg.get("IP_CLIENTES", "127.0.0.1")
    base_notif = int(cfg.get("PUERTO_NOTIF_BASE", "6001"))
    logs, procesos = [], []
    print(f"\n[carga] {args.clientes} clientes a {args.tpm} TPM c/u, mezcla {pct_cons}% consulta / {pct_mod}% "
          f"modificación / {pct_canc}% cancelación / {100 - pct_cons - pct_mod - pct_canc}% reserva")
    for i in range(1, args.clientes + 1):
        ruta_log = os.path.join(carpeta, f"cli{i}.log")
        logs.append(ruta_log)
        comando = [binario, f"cli{i}", f"tcp://{ip_gt}:{puerto_gt}", ip_clientes, str(base_notif + i), "aleatorio",
                   str(args.tpm), str(pct_cons), str(D), str(1000 + i), str(pct_mod), str(pct_canc)]
        procesos.append(subprocess.Popen(comando, stdout=open(ruta_log, "w", encoding="utf-8"), stderr=subprocess.STDOUT,
                                         cwd=proyecto))
    inicio = time.monotonic()

    def esperar_hasta(t_rel):
        """Duerme hasta t_rel segundos desde el inicio, mostrando el estado de la carga cada 5 s."""
        proximo_estado = 0.0
        while True:
            ahora = time.monotonic() - inicio
            if ahora >= t_rel:
                return
            if ahora >= proximo_estado:
                total, err = resumen_en_vivo(logs)
                marca = rojo(f"errores={err}") if err else verde("errores=0")
                print(f"   [t={ahora:5.1f}s] solicitudes completadas={total}  {marca}")
                proximo_estado = ahora + 5.0
            time.sleep(0.2)

    # --- inyección de fallas
    resultados_eventos = []
    try:
        for frac, nombre, objetivos in eventos:
            esperar_hasta(frac * D)
            t_evento = time.monotonic() - inicio
            antes = {o.nombre: o.pids() for o in objetivos}
            print(negrita(rojo(f"\n>>> [t={t_evento:5.1f}s] {nombre}")))
            for o in objetivos:
                print(f"    kill -9 {o.nombre} (PID {','.join(map(str, antes[o.nombre])) or '?'})")
                o.matar(antes[o.nombre])
            t0 = time.monotonic()
            pendientes = {o.nombre: o for o in objetivos}
            recuperacion = {}
            while pendientes and time.monotonic() - t0 < args.max_recuperacion:
                for nombre_p, p in list(pendientes.items()):
                    nuevos = p.pids()
                    if nuevos and not set(nuevos) & set(antes[nombre_p]):
                        recuperacion[nombre_p] = time.monotonic() - t0
                        print(verde(f"    {nombre_p} repuesto por el supervisor (PID {','.join(map(str, nuevos))}) "
                                    f"en {recuperacion[nombre_p]:.1f} s"))
                        del pendientes[nombre_p]
                time.sleep(0.1)
            for nombre_p in pendientes:
                print(rojo(f"    {nombre_p} NO fue repuesto tras {args.max_recuperacion:.0f} s"))
            resultados_eventos.append({"t": round(t_evento, 1), "evento": nombre,
                                       "objetivos": [o.nombre for o in objetivos],
                                       "recuperacion_s": {k: round(v, 2) for k, v in recuperacion.items()},
                                       "no_repuestos": list(pendientes)})
        esperar_hasta(D)
        print("\n[carga] tiempo cumplido; esperando a que los clientes terminen y reciban notificaciones...")
        for p in procesos:
            p.wait(timeout=60)
    except KeyboardInterrupt:
        for p in procesos:
            p.terminate()
        sys.exit("\n[cancelado]")
    time.sleep(3)

    # --- verificación
    datos = analizar_logs(logs)
    pt = datos["por_tipo"]
    err_sync = sum(pt[t]["ERROR"] for t in SINCRONAS)
    ok_res = pt["RESERVAR"]["OK"]
    acks = pt["CANCELAR"]["ACK"]
    notif_total = sum(datos["notif"].values())
    lentas = sum(1 for x in datos["latencias"] if x > 1000)
    bd = {"reservas": None, "canceladas": None, "consistente": None}
    if cmd_sql is not None:
        bd["reservas"] = entero_sql(cmd_sql, env_sql, SQL_RESERVAS)
        bd["canceladas"] = entero_sql(cmd_sql, env_sql, SQL_CANCELADAS)
        bd["consistente"] = entero_sql(cmd_sql, env_sql, SQL_CONSISTENTE)

    no_repuestos = [n for e in resultados_eventos for n in e["no_repuestos"]]
    recup_max = max((v for e in resultados_eventos for v in e["recuperacion_s"].values()), default=None)
    veredictos = []  # (nombre, estado, detalle)

    veredictos.append(("TRANSPARENCIA para el cliente", "PASS" if err_sync == 0 else "FAIL",
                       f"0 respuestas ERROR en consulta/reserva/modificación "
                       f"({len(datos['latencias'])} respuestas válidas recibidas)"
                       if err_sync == 0 else
                       f"{err_sync} respuestas ERROR: " + "; ".join(f"{k} x{v}" for k, v in datos["errores"].most_common(3))))
    veredictos.append(("RECUPERACIÓN de procesos", "PASS" if not no_repuestos else "FAIL",
                       f"todos repuestos; el más lento tardó {recup_max:.1f} s" if not no_repuestos and recup_max is not None
                       else f"no repuestos: {', '.join(no_repuestos)}"))
    if bd["consistente"] is None:
        veredictos.append(("CONSISTENCIA de la BD", "SKIP", "sin acceso a la BD"))
        veredictos.append(("EXACTAMENTE UNA VEZ (reservas)", "SKIP", "sin acceso a la BD"))
    else:
        veredictos.append(("CONSISTENCIA de la BD", "PASS" if bd["consistente"] == 1 else "FAIL",
                           "asientos ocupados == asientos de reservas activas" if bd["consistente"] == 1
                           else "INCONSISTENTE: ocupados != asientos de reservas activas"))
        if args.sin_reset:
            veredictos.append(("EXACTAMENTE UNA VEZ (reservas)", "SKIP", "se usó --sin-reset"))
        elif bd["reservas"] == ok_res:
            veredictos.append(("EXACTAMENTE UNA VEZ (reservas)", "PASS",
                               f"{ok_res} reservas OK en clientes == {bd['reservas']} filas en BD "
                               f"(los reintentos no duplicaron ni perdieron nada)"))
        elif bd["reservas"] is not None and bd["reservas"] > ok_res:
            veredictos.append(("EXACTAMENTE UNA VEZ (reservas)", "FAIL",
                               f"BD tiene {bd['reservas']} filas pero los clientes vieron {ok_res} OK: "
                               f"reservas confirmadas sin que el cliente lo supiera"))
        else:
            veredictos.append(("EXACTAMENTE UNA VEZ (reservas)", "FAIL",
                               f"BD tiene {bd['reservas']} filas y los clientes vieron {ok_res} OK: reservas perdidas"))
    if acks == notif_total:
        veredictos.append(("NOTIFICACIONES de cancelación", "PASS", f"{acks} ACK == {notif_total} notificaciones"))
    else:
        esperado = args.incluir_cancelacion
        veredictos.append(("NOTIFICACIONES de cancelación", "WARN",
                           f"{acks} ACK vs {notif_total} notificaciones ({acks - notif_total:+d}). "
                           + ("Esperado: se mató una réplica de Cancelación y sus mensajes en cola se pierden "
                              "(limitación documentada en el README)." if esperado else
                              "Revisar: no se mató Cancelación; puede que falten segundos de espera o haya pérdida.")))

    # --- informe en consola y en Markdown
    print("\n" + negrita("=" * 78))
    print(negrita(" RESULTADO"))
    print(negrita("=" * 78))
    for e in resultados_eventos:
        rec = ", ".join(f"{k}: {v:.1f} s" for k, v in e["recuperacion_s"].items()) or "—"
        print(f" t={e['t']:>5.1f}s  {e['evento']:<48} repuesto en: {rec}")
    print()
    print(f" Solicitudes: " + "  ".join(f"{t}={sum(pt[t].values())}" for t in pt) +
          f"  |  con latencia > 1 s (absorbidas por reintento): {lentas}  |  máx: "
          f"{max(datos['latencias']) if datos['latencias'] else 0:.0f} ms  |  p99: "
          f"{percentil(datos['latencias'], 0.99) or 0:.0f} ms")
    print()
    estilo = {"PASS": verde, "FAIL": rojo, "WARN": amarillo, "SKIP": amarillo}
    for nombre, estado, detalle in veredictos:
        print(f" [{estilo[estado](estado)}] {nombre:<32} {detalle}")
    fallo = any(v[1] == "FAIL" for v in veredictos)
    print("\n " + (rojo(negrita("PRUEBA NO SUPERADA")) if fallo else verde(negrita("PRUEBA SUPERADA: las fallas fueron transparentes."))))

    md = ["# Prueba de tolerancia a fallas", "",
          f"- Fecha: {datetime.datetime.now().isoformat(timespec='seconds')}",
          f"- Carga: {args.clientes} clientes × {args.tpm} TPM durante {D} s "
          f"(mezcla {pct_cons}% consulta / {pct_mod}% modificación / {pct_canc}% cancelación / "
          f"{100 - pct_cons - pct_mod - pct_canc}% reserva)",
          f"- Topología: RMC×{n_rmc}, Cancelación×{n_canc}, GT en {destino_gt.descripcion()}, "
          f"negocio en {destino_neg.descripcion()}", "",
          "## Fallas inyectadas (`kill -9`)", "",
          "| t (s) | Falla | Procesos | Tiempo de reposición |", "|---:|---|---|---|"]
    for e in resultados_eventos:
        rec = ", ".join(f"{k}: {v:.1f} s" for k, v in e["recuperacion_s"].items()) or "NO repuesto"
        md.append(f"| {e['t']} | {e['evento']} | {', '.join(e['objetivos'])} | {rec} |")
    md += ["", "## Verificaciones", "", "| Verificación | Resultado | Detalle |", "|---|:---:|---|"]
    for nombre, estado, detalle in veredictos:
        md.append(f"| {nombre} | {estado} | {detalle} |")
    md += ["", "## Conteos", "", "| Operación | OK | ACK | RECHAZADO | ERROR |", "|---|---:|---:|---:|---:|"]
    for t, c in pt.items():
        md.append(f"| {t} | {c['OK']} | {c['ACK']} | {c['RECHAZADO']} | {c['ERROR']} |")
    md += ["", f"- Notificaciones de cancelación recibidas: {notif_total} (OK: {datos['notif']['OK']}, "
               f"ERROR: {datos['notif']['ERROR']})",
           f"- Solicitudes con latencia > 1 s (reintentos absorbidos): {lentas}",
           f"- BD: {bd['reservas']} filas en `reserva`, {bd['canceladas']} canceladas, consistente={bd['consistente']}", ""]
    with open(os.path.join(carpeta, "informe_fallas.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    with open(os.path.join(carpeta, "eventos.json"), "w", encoding="utf-8") as f:
        json.dump({"eventos": resultados_eventos, "bd": bd,
                   "veredictos": [{"nombre": n, "estado": e, "detalle": d} for n, e, d in veredictos]},
                  f, indent=2, ensure_ascii=False)
    print(f"\n Informe y logs en: {carpeta}")
    sys.exit(1 if fallo else 0)


if __name__ == "__main__":
    main()
