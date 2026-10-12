#!/usr/bin/env bash
# empaquetar.sh - Genera un archivo ZIP con el proyecto sin incluir el directorio .git ni binarios

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$DIR")"
PROJECT_NAME="$(basename "$DIR")"
ZIP_DESTINO="$PARENT_DIR/${PROJECT_NAME}.zip"

echo "[-] Generando archivo ZIP: $ZIP_DESTINO"

# Nos movemos al directorio padre para empaquetar con el nombre de la carpeta raíz
cd "$PARENT_DIR"

rm -f "$ZIP_DESTINO"

zip -r "$ZIP_DESTINO" "$PROJECT_NAME" \
    -x "${PROJECT_NAME}/.git/*" \
    -x "${PROJECT_NAME}/.git" \
    -x "${PROJECT_NAME}/*.o" \
    -x "${PROJECT_NAME}/*.log" \
    -x "${PROJECT_NAME}/capa_Consumidores/cliente" \
    -x "${PROJECT_NAME}/capa_Servicio-Almacenamiento/persistencia" \
    -x "${PROJECT_NAME}/capa_Servicios-Negocio/servicio_Cancelacion/cancelacion" \
    -x "${PROJECT_NAME}/capa_Servicios-Negocio/servicio_GT/gestor" \
    -x "${PROJECT_NAME}/capa_Servicios-Negocio/servicio_RMC/rmc" \
    -x "${PROJECT_NAME}/*.zip" \
    -x "${PROJECT_NAME}/*.tar.gz"

echo "[+] Proyecto empaquetado exitosamente en: $ZIP_DESTINO"
