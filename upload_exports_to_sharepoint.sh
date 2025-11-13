#!/usr/bin/env bash
# Upload des exports CSV vers SharePoint via rclone
# Usage :
#   ./upload_exports_to_sharepoint.sh [local_dir]

set -euo pipefail

LOCAL_DIR="${1:-exports}"
REMOTE="${RCLONE_REMOTE_EXPORT:-sharepoint_bi:General/PowerBi/csv}"
VERSIONED="${VERSIONED:-0}"

if [ ! -d "$LOCAL_DIR" ]; then
  echo "Erreur: dossier local '$LOCAL_DIR' introuvable. Exécutez d'abord l'export DWH."
  exit 2
fi

if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone introuvable. Installez rclone puis configurez un remote SharePoint."
  exit 3
fi

if [ "$VERSIONED" -eq 1 ]; then
  today=$(date +%Y%m%d)
  target="${REMOTE}/${today}"
  echo "Upload versionné: ${LOCAL_DIR} -> ${target}"
  rclone copy "$LOCAL_DIR" "$target" --progress
else
  echo "Synchronisation: ${LOCAL_DIR} -> ${REMOTE}"
  rclone sync "$LOCAL_DIR" "$REMOTE" --progress
fi
echo "Upload terminé avec succès."