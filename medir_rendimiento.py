#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
medir_rendimiento.py - Medición automática de rendimiento del SRB (Tabla 1 del enunciado)

Qué hace
--------
1. `correr`   : ejecuta los escenarios de 3, 5 y 8 clientes (lanzar_clientes.sh --escenario N),
                reseteando la BD antes de cada corrida, y guarda los logs de cada cliente.
2. `analizar` : lee todos los logs guardados y genera, para base vs escalable:
                  - resumen.csv / resumen_por_corrida.csv
                  - tabla_1.md  (tablas listas para pegar en el informe + % de mejora)
                  - 4 gráficas PNG (si matplotlib está instalado)
3. `info`     : guarda hw/sw de ESTA máquina (el informe exige especificar hw y sw de las pruebas).

Flujo típico (se repite dos veces, cambiando solo la configuración del sistema):
    # 1) Levantar la arquitectura BASE (REPLICAS_RMC=1, REPLICAS_CANCELACION=1, sin réplica de BD)
    ./medir_rendimiento.py correr --etiqueta base --tpm 600 --duracion 150
    # 2) Levantar la arquitectura ESCALABLE (3 RMC, 2 Cancelación, réplica de lectura)
    ./medir_rendimiento.py correr --etiqueta escalable --tpm 600 --duracion 150
    # 3) Tablas y gráficas (también se generan al final de cada `correr`)
    ./medir_rendimiento.py analizar

Se ejecuta en la máquina de los CLIENTES. Solo usa la librería estándar de Python 3.
(Para las gráficas: `pip install matplotlib` o `sudo apt install python3-matplotlib`.)

Cómo se calculan las métricas (explícalo en el informe)
-------------------------------------------------------
- Tiempo de respuesta: medido en el cliente, de extremo a extremo (envío -> respuesta del GT).
  Promedio y desviación estándar POBLACIONAL (divide entre n, igual que el resumen de tus programas),
  calculados sobre todas las solicitudes de todos los clientes del escenario (no promedio de promedios).
- Las respuestas `ERROR|...` (timeouts) NO entran en el promedio; se cuentan aparte en la columna
  de errores, para que un fallo no se esconda ni distorsione la latencia. Con --incluir-errores sí entran.
- TPM por tipo de operación = suma, sobre los clientes, de (solicitudes procesadas / minutos de su corrida).
  Equivale al caudal que completó el GT. Si hay acceso a la BD, se añade el TPM de reservas medido en la BD
  (filas de la tabla `reserva` / minutos) como contraste, ya que el enunciado permite medirlo ahí.
"""

import argparse
import csv
import datetime
import glob
import json
import math
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time

TIPOS = ["CONSULTA", "RESERVAR", "MODIFICAR", "CANCELAR"]

RE_SOLICITUD = re.compile(r"^(CONSULTA|RESERVAR|MODIFICAR|CANCELAR)\|.*?  ->  (.*)  \(([0-9.]+) ms\)\s*$")
RE_RESUMEN = re.compile(r"=== Resumen \(([0-9.]+) s\) ===")
RE_ENTERO = re.compile(r"^\s*-?\d+\s*$")


# ----------------------------------------------------------------------------- utilidades

def leer_config(proyecto):
    """Lee config.env (KEY=VALUE, con comentarios '#')."""
    cfg = {}
    ruta = os.path.join(proyecto, "config.env")
    if os.path.isfile(ruta):
        with open(ruta, encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if not linea or linea.startswith("#") or "=" not in linea:
                    continue
                clave, valor = linea.split("=", 1)
                valor = re.split(r"\s+#", valor, maxsplit=1)[0].strip().strip('"').strip("'")
                cfg[clave.strip()] = valor
    return cfg


def puerto_abierto(ip, puerto, espera=2.0):
    try:
        with socket.create_connection((ip, int(puerto)), timeout=espera):
            return True
    except OSError:
        return False


def comando_sql(cfg, reset_cmd):
    """Devuelve (comando_shell, env) para ejecutar SQL por stdin, o (None, None) si no hay forma."""
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
        host = cfg.get("IP_PERSISTENCIA", "127.0.0.1")
        puerto = cfg.get("PUERTO_POSTGRES", "5434")
        return f"psql -q -h {host} -p {puerto} -U {usuario} -d {base} -v ON_ERROR_STOP=1", env
    return None, None


def ejecutar_sql(cmd, env, sql):
    return subprocess.run(cmd, shell=True, input=sql, capture_output=True, text=True, env=env, timeout=120)


def entero_sql(cmd, env, sql):
    """Ejecuta una consulta que devuelve UN entero y lo extrae (tolera formato de psql con o sin -At)."""
    r = ejecutar_sql(cmd, env, sql)
    if r.returncode != 0:
        return None
    for linea in r.stdout.splitlines():
        if RE_ENTERO.match(linea):
            return int(linea)
    return None


SQL_RESERVAS = "SELECT count(*) FROM reserva;"
SQL_CONSISTENTE = (
    "SELECT CASE WHEN (SELECT COALESCE(sum(ocupados),0) FROM evento) = "
    "(SELECT COALESCE(sum(asientos),0) FROM reserva WHERE estado='ACTIVA') THEN 1 ELSE 0 END;"
)


def estadisticas(muestras):
    """Promedio, desviación estándar poblacional, p50, p95 y máximo."""
    n = len(muestras)
    if n == 0:
        return None
    media = sum(muestras) / n
    desv = math.sqrt(sum((x - media) ** 2 for x in muestras) / n)
    orden = sorted(muestras)

    def percentil(p):
        return orden[min(n - 1, max(0, math.ceil(p * n) - 1))]

    return {"n": n, "prom": media, "desv": desv, "p50": percentil(0.50), "p95": percentil(0.95), "max": orden[-1]}


# ----------------------------------------------------------------------------- hw / sw

def ejecutar_texto(cmd):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
        return (r.stdout or r.stderr).strip()
    except Exception as e:  # pragma: no cover
        return f"(no disponible: {e})"


def info_sistema():
    lineas = [f"Fecha: {datetime.datetime.now().isoformat(timespec='seconds')}",
              f"Host: {socket.gethostname()}",
              f"SO: {ejecutar_texto('. /etc/os-release 2>/dev/null && echo $PRETTY_NAME') or platform.platform()}",
              f"Kernel: {platform.release()} ({platform.machine()})"]
    modelo = ejecutar_texto("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2")
    lineas.append(f"CPU: {modelo.strip() or platform.processor()}  |  núcleos lógicos: {os.cpu_count()}")
    memoria = ejecutar_texto("grep -m1 MemTotal /proc/meminfo | awk '{printf \"%.1f GiB\", $2/1048576}'")
    lineas.append(f"RAM: {memoria}")
    lineas.append(f"Compilador: {ejecutar_texto('g++ --version | head -1')}")
    lineas.append(f"ZeroMQ (libzmq): {ejecutar_texto('pkg-config --modversion libzmq 2>/dev/null') or '(desconocida)'}")
    lineas.append(f"libpqxx: {ejecutar_texto('pkg-config --modversion libpqxx 2>/dev/null') or '(desconocida)'}")
    lineas.append(f"Docker: {ejecutar_texto('docker --version 2>/dev/null') or '(no instalado)'}")
    lineas.append(f"Python: {platform.python_version()}")
    return "\n".join(lineas) + "\n"


def guardar_info(directorio):
    os.makedirs(directorio, exist_ok=True)
    ruta = os.path.join(directorio, f"info_sistema_{socket.gethostname()}.txt")
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(info_sistema())
    return ruta


# ----------------------------------------------------------------------------- correr

def correr(args):
    proyecto = os.path.abspath(args.proyecto)
    cfg = leer_config(proyecto)
    lanzador = os.path.join(proyecto, "lanzar_clientes.sh")
    if not os.path.isfile(lanzador):
        sys.exit(f"No encuentro {lanzador}. Usa --proyecto <directorio del proyecto>.")

    ip_gt = cfg.get("IP_GT", "127.0.0.1")
    puerto_gt = cfg.get("PUERTO_GT", "5555")
    if not puerto_abierto(ip_gt, puerto_gt):
        sys.exit(f"El GT no responde en {ip_gt}:{puerto_gt}. Levanta el sistema antes de medir "
                 f"(./iniciar_capa.sh persistencia | negocio | gt).")

    escenarios = [int(e) for e in args.escenarios.split(",") if e.strip()]
    for e in escenarios:
        if e not in (3, 5, 8):
            sys.exit(f"Escenario inválido: {e}. Los de la Tabla 1 son 3, 5 y 8.")

    schema = os.path.abspath(args.schema) if args.schema else os.path.join(proyecto, "schema.sql")
    cmd_sql, env_sql = (None, None)
    if not args.sin_reset or not args.sin_bd:
        cmd_sql, env_sql = comando_sql(cfg, args.reset_cmd)
    if not args.sin_reset:
        if cmd_sql is None:
            sys.exit("No puedo resetear la BD (sin docker local ni psql). Opciones: --reset-cmd \"<comando>\" "
                     "(recibe el SQL por stdin, p. ej. \"ssh m3 'docker exec -i srb_postgres psql -U postgres -d srb'\") "
                     "o --sin-reset si la reseteas a mano entre corridas.")
        if not os.path.isfile(schema):
            sys.exit(f"No encuentro el esquema {schema}")

    salida = os.path.abspath(args.salida)
    dir_etiqueta = os.path.join(salida, args.etiqueta)
    os.makedirs(dir_etiqueta, exist_ok=True)
    print(f"[info] hw/sw de esta máquina -> {guardar_info(salida)}")
    # Copia de la configuración (sin contraseñas) para documentar con qué topología se midió
    ruta_cfg = os.path.join(proyecto, "config.env")
    if os.path.isfile(ruta_cfg):
        with open(ruta_cfg, encoding="utf-8") as origen, \
                open(os.path.join(dir_etiqueta, "config.env.txt"), "w", encoding="utf-8") as destino:
            for linea in origen:
                destino.write(re.sub(r"(PASSWORD\s*=).*", r"\1***", linea))

    print("=" * 78)
    print(f" Medición '{args.etiqueta}': escenarios {escenarios} | {args.tpm} TPM/cliente | "
          f"{args.duracion} s | {args.repeticiones} repetición(es)")
    print(f" GT: {ip_gt}:{puerto_gt}  |  reset de BD: {'NO' if args.sin_reset else 'antes de cada corrida'}")
    print("=" * 78)
    print(f" Verifica que el sistema esté levantado con la configuración «{args.etiqueta}» "
          f"(réplicas RMC/Cancelación y réplica de BD).")
    if not args.si:
        input(" Pulsa Enter para comenzar (Ctrl+C para cancelar)... ")

    total = len(escenarios) * args.repeticiones
    contador = 0
    for escenario in escenarios:
        for rep in range(1, args.repeticiones + 1):
            contador += 1
            print(f"\n--- [{contador}/{total}] escenario {escenario} clientes, repetición {rep} ---")
            destino = os.path.join(dir_etiqueta, f"esc{escenario}_rep{rep}")
            if os.path.isdir(destino):
                shutil.rmtree(destino)
            os.makedirs(destino)

            if not args.sin_reset:
                with open(schema, encoding="utf-8") as f:
                    r = ejecutar_sql(cmd_sql, env_sql, f.read())
                if r.returncode != 0:
                    sys.exit(f"Falló el reset de la BD:\n{r.stderr}")
                print("[reset] BD restaurada al estado inicial (schema.sql)")
                time.sleep(args.espera_reset)  # deja que la réplica de lectura alcance a la primaria

            for vieja in glob.glob("/tmp/srb_cli[0-9]*.log"):
                os.remove(vieja)

            inicio = datetime.datetime.now()
            rc = None
            try:
                r = subprocess.run(["bash", lanzador, "--escenario", str(escenario), str(args.tpm), str(args.duracion)],
                                   capture_output=True, text=True, cwd=proyecto,
                                   timeout=args.duracion + 120)
                rc = r.returncode
                with open(os.path.join(destino, "lanzador.txt"), "w", encoding="utf-8") as f:
                    f.write(r.stdout + r.stderr)
            except subprocess.TimeoutExpired:
                print("[aviso] la corrida excedió el tiempo máximo; se recogen los logs parciales")

            for log in sorted(glob.glob("/tmp/srb_cli[0-9]*.log")):
                shutil.copy(log, destino)

            meta = {"etiqueta": args.etiqueta, "escenario": escenario, "repeticion": rep, "tpm": args.tpm,
                    "duracion": args.duracion, "inicio": inicio.isoformat(timespec="seconds"), "rc": rc,
                    "bd_reservas": None, "bd_consistente": None}
            if cmd_sql is not None and not args.sin_bd:
                time.sleep(2)
                meta["bd_reservas"] = entero_sql(cmd_sql, env_sql, SQL_RESERVAS)
                meta["bd_consistente"] = entero_sql(cmd_sql, env_sql, SQL_CONSISTENTE)
            with open(os.path.join(destino, "meta.json"), "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)

            run = parsear_corrida(destino)
            print(f"[ok] clientes={len(run['clientes'])}  " + "  ".join(
                f"{t}: n={run['tipos'][t]['procesadas']} err={run['tipos'][t]['errores']} "
                f"tpm={run['tipos'][t]['tpm']:.0f}" for t in TIPOS if run['tipos'][t]['total'] > 0))
            if meta["bd_consistente"] == 0:
                print("[ALERTA] la BD quedó INCONSISTENTE tras esta corrida")
            ok_res = run["tipos"]["RESERVAR"]["ok"]
            if meta["bd_reservas"] is not None and meta["bd_reservas"] != ok_res:
                print(f"[ALERTA] la BD tiene {meta['bd_reservas']} reservas pero los clientes recibieron {ok_res} OK. "
                      f"Si hubo errores, alguna reserva pudo confirmarse sin que el cliente lo supiera; "
                      f"si no los hubo, el reset de la BD no se aplicó o hay otros clientes activos.")
            if contador < total:
                time.sleep(args.pausa)

    print("\n[+] Corridas terminadas. Generando tablas y gráficas...")
    analizar(argparse.Namespace(salida=salida, incluir_errores=args.incluir_errores))


# ----------------------------------------------------------------------------- análisis

def parsear_cliente(ruta, ventana_defecto):
    info = {"muestras": {t: [] for t in TIPOS}, "errores": {t: 0 for t in TIPOS}, "ok": {t: 0 for t in TIPOS},
            "ventana": None}
    with open(ruta, encoding="utf-8", errors="replace") as f:
        for linea in f:
            m = RE_SOLICITUD.match(linea)
            if m:
                tipo, respuesta, ms = m.group(1), m.group(2), float(m.group(3))
                if respuesta.startswith("ERROR"):
                    info["errores"][tipo] += 1
                    info.setdefault("muestras_error", {t: [] for t in TIPOS})[tipo].append(ms)
                else:
                    info["muestras"][tipo].append(ms)
                    if respuesta.startswith("OK|"):
                        info["ok"][tipo] += 1
                continue
            m = RE_RESUMEN.search(linea)
            if m:
                info["ventana"] = float(m.group(1))
    if not info["ventana"]:
        info["ventana"] = float(ventana_defecto)
    return info


def parsear_corrida(directorio):
    with open(os.path.join(directorio, "meta.json"), encoding="utf-8") as f:
        meta = json.load(f)
    clientes = {}
    for ruta in sorted(glob.glob(os.path.join(directorio, "srb_cli[0-9]*.log"))):
        clientes[os.path.basename(ruta)[4:-4]] = parsear_cliente(ruta, meta["duracion"])
    tipos = {}
    for t in TIPOS:
        muestras, errores_ms, errores, tpm, ok = [], [], 0, 0.0, 0
        for c in clientes.values():
            muestras += c["muestras"][t]
            errores += c["errores"][t]
            ok += c["ok"][t]
            errores_ms += c.get("muestras_error", {}).get(t, [])
            tpm += len(c["muestras"][t]) / (c["ventana"] / 60.0)
        tipos[t] = {"muestras": muestras, "errores_ms": errores_ms, "errores": errores, "ok": ok,
                    "procesadas": len(muestras), "total": len(muestras) + errores, "tpm": tpm}
    ventana = max((c["ventana"] for c in clientes.values()), default=float(meta["duracion"]))
    return {"meta": meta, "clientes": clientes, "tipos": tipos, "ventana": ventana}


def cargar_corridas(salida):
    corridas = []
    for meta in sorted(glob.glob(os.path.join(salida, "*", "esc*_rep*", "meta.json"))):
        corridas.append(parsear_corrida(os.path.dirname(meta)))
    return corridas


def agregar(corridas, incluir_errores):
    """agg[etiqueta][escenario][tipo] -> métricas agregadas sobre las repeticiones."""
    agrupado = {}
    for c in corridas:
        agrupado.setdefault(c["meta"]["etiqueta"], {}).setdefault(c["meta"]["escenario"], []).append(c)
    agg = {}
    for etiqueta, por_esc in agrupado.items():
        for escenario, runs in por_esc.items():
            fila = {"_reps": len(runs)}
            for t in TIPOS:
                muestras = []
                for r in runs:
                    muestras += r["tipos"][t]["muestras"]
                    if incluir_errores:
                        muestras += r["tipos"][t]["errores_ms"]
                est = estadisticas(muestras) or {"n": 0, "prom": None, "desv": None, "p50": None, "p95": None, "max": None}
                est["errores"] = sum(r["tipos"][t]["errores"] for r in runs)
                est["total"] = sum(r["tipos"][t]["total"] for r in runs)
                est["tpm"] = sum(r["tipos"][t]["tpm"] for r in runs) / len(runs)
                fila[t] = est
            reservas_bd = [r["meta"]["bd_reservas"] for r in runs if r["meta"].get("bd_reservas") is not None]
            ventanas = [r["ventana"] for r in runs if r["meta"].get("bd_reservas") is not None]
            fila["_tpm_reserva_bd"] = (sum(rb / (v / 60.0) for rb, v in zip(reservas_bd, ventanas)) / len(reservas_bd)
                                       if reservas_bd else None)
            consist = [r["meta"]["bd_consistente"] for r in runs if r["meta"].get("bd_consistente") is not None]
            fila["_consistente"] = (all(x == 1 for x in consist) if consist else None)
            fila["_ok_reservas"] = sum(r["tipos"]["RESERVAR"]["ok"] for r in runs)
            fila["_bd_reservas"] = (sum(reservas_bd) if reservas_bd else None)
            agg.setdefault(etiqueta, {})[escenario] = fila
    return agg


def fmt(valor, decimales=1):
    return "—" if valor is None else f"{valor:.{decimales}f}"


def fmt_media_desv(est):
    if not est or est["prom"] is None:
        return "—"
    return f"{est['prom']:.1f} ± {est['desv']:.1f}"


def mejora(base, esc, menor_es_mejor):
    if base is None or esc is None or base == 0:
        return "—"
    pct = (base - esc) / base * 100.0 if menor_es_mejor else (esc - base) / base * 100.0
    return "0.0 %" if abs(pct) < 0.05 else f"{pct:+.1f} %"


def escribir_csv(agg, corridas, salida):
    ruta = os.path.join(salida, "resumen.csv")
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["arquitectura", "clientes", "operacion", "repeticiones", "n_procesadas", "errores",
                    "prom_ms", "desv_ms", "p50_ms", "p95_ms", "max_ms", "tpm", "tpm_reserva_bd", "bd_consistente"])
        for etiqueta in sorted(agg):
            for escenario in sorted(agg[etiqueta]):
                fila = agg[etiqueta][escenario]
                for t in TIPOS:
                    e = fila[t]
                    if e["total"] == 0:
                        continue
                    w.writerow([etiqueta, escenario, t, fila["_reps"], e["n"], e["errores"],
                                fmt(e["prom"], 3), fmt(e["desv"], 3), fmt(e["p50"], 3), fmt(e["p95"], 3),
                                fmt(e["max"], 3), fmt(e["tpm"], 2),
                                fmt(fila["_tpm_reserva_bd"], 2) if t == "RESERVAR" else "", fila["_consistente"]])
    ruta2 = os.path.join(salida, "resumen_por_corrida.csv")
    with open(ruta2, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["arquitectura", "clientes", "repeticion", "operacion", "n_procesadas", "errores", "prom_ms",
                    "desv_ms", "tpm", "ventana_s", "bd_reservas", "bd_consistente"])
        for c in corridas:
            m = c["meta"]
            for t in TIPOS:
                d = c["tipos"][t]
                if d["total"] == 0:
                    continue
                est = estadisticas(d["muestras"])
                w.writerow([m["etiqueta"], m["escenario"], m["repeticion"], t, d["procesadas"], d["errores"],
                            fmt(est["prom"], 3) if est else "", fmt(est["desv"], 3) if est else "",
                            fmt(d["tpm"], 2), fmt(c["ventana"], 1), m.get("bd_reservas"), m.get("bd_consistente")])
    return ruta, ruta2


METRICAS = [
    ("1. Consulta: tiempo de respuesta promedio ± desv. estándar (ms)", "CONSULTA", "ms", True),
    ("2. Reserva: tiempo de respuesta promedio ± desv. estándar (ms)", "RESERVAR", "ms", True),
    ("3. Consulta: transacciones por minuto (TPM)", "CONSULTA", "tpm", False),
    ("4. Reserva: transacciones por minuto (TPM)", "RESERVAR", "tpm", False),
]


def escribir_tablas(agg, salida):
    etiquetas = sorted(agg)
    escenarios = sorted({e for et in agg.values() for e in et})
    base, esc = "base", "escalable"
    lineas = ["# Resultados de rendimiento (Tabla 1)", "",
              "Mejora: positiva = la arquitectura escalable es mejor (menos tiempo / más TPM).", ""]
    if base not in agg or esc not in agg:
        lineas += [f"> Aviso: solo hay datos de {etiquetas}. Para comparar corre `correr --etiqueta base` "
                   f"y `correr --etiqueta escalable`.", ""]

    def celda(et, n, tipo, modo):
        fila = agg.get(et, {}).get(n)
        if not fila or fila[tipo]["total"] == 0:
            return "—", None
        e = fila[tipo]
        if modo == "ms":
            return fmt_media_desv(e), e["prom"]
        return fmt(e["tpm"], 1), e["tpm"]

    for titulo, tipo, modo, menor in METRICAS:
        lineas += [f"## {titulo}", "", "| Clientes | Base | Escalable | Mejora |", "|---:|---:|---:|---:|"]
        for n in escenarios:
            txt_b, val_b = celda(base, n, tipo, modo)
            txt_e, val_e = celda(esc, n, tipo, modo)
            lineas.append(f"| {n} | {txt_b} | {txt_e} | {mejora(val_b, val_e, menor)} |")
        lineas.append("")

    lineas += ["## Complementarias", "",
               "| Clientes | Arq. | Consulta p95 (ms) | Reserva p95 (ms) | Modificación prom ± desv (ms) | "
               "TPM modificación | TPM reserva medido en BD | Reservas OK (clientes / BD) | "
               "Errores (cons/res/mod) | BD consistente |",
               "|---:|---|---:|---:|---:|---:|---:|---:|---|:---:|"]
    for n in escenarios:
        for et in etiquetas:
            fila = agg.get(et, {}).get(n)
            if not fila:
                continue
            cons, res, mod = fila["CONSULTA"], fila["RESERVAR"], fila["MODIFICAR"]
            lineas.append(
                f"| {n} | {et} | {fmt(cons['p95'])} | {fmt(res['p95'])} | {fmt_media_desv(mod) if mod['total'] else '—'} | "
                f"{fmt(mod['tpm']) if mod['total'] else '—'} | {fmt(fila['_tpm_reserva_bd'])} | "
                f"{fila['_ok_reservas']} / {'—' if fila['_bd_reservas'] is None else fila['_bd_reservas']} | "
                f"{cons['errores']}/{res['errores']}/{mod['errores']} | "
                f"{'—' if fila['_consistente'] is None else ('sí' if fila['_consistente'] else '**NO**')} |")
    lineas.append("")
    hay_errores = any(f[t]["errores"] for et in agg.values() for f in et.values() for t in TIPOS)
    if hay_errores:
        lineas += ["> **Atención:** hubo respuestas `ERROR|...` (timeouts). No entran en los promedios de tiempo "
                   "(ver columna de errores); un sistema que satura debe discutirse en el análisis.", ""]
    ruta = os.path.join(salida, "tabla_1.md")
    with open(ruta, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))
    return ruta, "\n".join(lineas)


def graficar(agg, salida):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[aviso] matplotlib no está instalado: no se generan gráficas "
              "(pip install matplotlib  |  sudo apt install python3-matplotlib).")
        return []
    archivos = []
    escenarios = sorted({e for et in agg.values() for e in et})
    estilos = {"base": ("tab:red", "o", "--"), "escalable": ("tab:green", "s", "-")}
    nombres = ["grafica_1_tiempo_consulta.png", "grafica_2_tiempo_reserva.png",
               "grafica_3_tpm_consulta.png", "grafica_4_tpm_reserva.png"]
    for (titulo, tipo, modo, _), nombre in zip(METRICAS, nombres):
        fig, ax = plt.subplots(figsize=(6.2, 4.0))
        hay = False
        for et in sorted(agg):
            xs, ys, es = [], [], []
            for n in escenarios:
                fila = agg[et].get(n)
                if fila and fila[tipo]["total"] > 0 and (fila[tipo]["prom"] is not None or modo == "tpm"):
                    xs.append(n)
                    ys.append(fila[tipo]["prom"] if modo == "ms" else fila[tipo]["tpm"])
                    es.append(fila[tipo]["desv"] if modo == "ms" else 0)
            if not xs:
                continue
            hay = True
            color, marcador, trazo = estilos.get(et, (None, "^", "-"))
            ax.errorbar(xs, ys, yerr=es if modo == "ms" else None, label=et, color=color, marker=marcador,
                        linestyle=trazo, capsize=4)
        if not hay:
            plt.close(fig)
            continue
        ax.set_xlabel("Número de clientes simultáneos")
        ax.set_ylabel("Tiempo de respuesta (ms)" if modo == "ms" else "Transacciones por minuto")
        ax.set_title(titulo.split(". ", 1)[1].replace(" promedio ± desv. estándar (ms)", ""), fontsize=10)
        ax.set_xticks(escenarios)
        ax.grid(alpha=0.3)
        ax.legend(title="Arquitectura")
        if modo == "ms":
            ax.set_ylim(bottom=0)
        fig.tight_layout()
        ruta = os.path.join(salida, nombre)
        fig.savefig(ruta, dpi=160)
        plt.close(fig)
        archivos.append(ruta)
    return archivos


def analizar(args):
    salida = os.path.abspath(args.salida)
    corridas = cargar_corridas(salida)
    if not corridas:
        sys.exit(f"No hay corridas en {salida} (se esperan carpetas <etiqueta>/esc<N>_rep<k>/).")
    agg = agregar(corridas, args.incluir_errores)
    csv1, csv2 = escribir_csv(agg, corridas, salida)
    ruta_md, texto = escribir_tablas(agg, salida)
    graficas = graficar(agg, salida)
    print(texto)
    print("\nArchivos generados:")
    for ruta in [csv1, csv2, ruta_md] + graficas:
        print(f"  {ruta}")


# ----------------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Medición de rendimiento del SRB (Tabla 1)")
    sub = ap.add_subparsers(dest="comando", required=True)

    c = sub.add_parser("correr", help="ejecuta los escenarios y analiza")
    c.add_argument("--etiqueta", required=True, help="'base' o 'escalable' (nombre de la arquitectura medida)")
    c.add_argument("--escenarios", default="3,5,8", help="lista separada por comas (por defecto 3,5,8)")
    c.add_argument("--tpm", type=int, default=600, help="solicitudes por minuto por cliente (por defecto 600)")
    c.add_argument("--duracion", type=int, default=150, help="segundos por corrida; el enunciado pide 2-3 min")
    c.add_argument("--repeticiones", type=int, default=1, help="repeticiones por escenario (recomendado >= 3)")
    c.add_argument("--proyecto", default=os.path.dirname(os.path.abspath(__file__)), help="directorio del proyecto")
    c.add_argument("--salida", default="resultados_rendimiento")
    c.add_argument("--schema", help="esquema SQL para el reset (por defecto <proyecto>/schema.sql, 50 eventos)")
    c.add_argument("--reset-cmd", help="comando que recibe SQL por stdin para resetear/consultar la BD "
                                       "(por defecto: docker exec local o psql)")
    c.add_argument("--sin-reset", action="store_true", help="no resetear la BD entre corridas (no recomendado)")
    c.add_argument("--sin-bd", action="store_true", help="no consultar la BD tras cada corrida")
    c.add_argument("--espera-reset", type=float, default=2.0, help="segundos de espera tras el reset")
    c.add_argument("--pausa", type=int, default=10, help="segundos de pausa entre corridas")
    c.add_argument("--incluir-errores", action="store_true", help="incluir respuestas ERROR en los promedios")
    c.add_argument("--si", action="store_true", help="no pedir confirmación")
    c.set_defaults(func=correr)

    a = sub.add_parser("analizar", help="regenera tablas, CSV y gráficas desde los logs guardados")
    a.add_argument("--salida", default="resultados_rendimiento")
    a.add_argument("--incluir-errores", action="store_true")
    a.set_defaults(func=analizar)

    i = sub.add_parser("info", help="guarda hw/sw de esta máquina (ejecútalo en M1, M2, M3 y clientes)")
    i.add_argument("--salida", default="resultados_rendimiento")
    i.set_defaults(func=lambda args: print(info_sistema(), "->", guardar_info(os.path.abspath(args.salida))))

    args = ap.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\n[cancelado]")
        sys.exit(130)


if __name__ == "__main__":
    main()
