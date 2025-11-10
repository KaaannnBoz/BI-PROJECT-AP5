#!/usr/bin/env bash
# Synchronise le dossier local `exports/` vers un emplacement SharePoint/OneDrive configuré avec rclone.
# Usage:
#   export RCLONE_REMOTE_EXPORT="sharepoint:Documents/ETL_clean"
#   ./upload_exports_to_sharepoint.sh [local_dir]

set -euo pipefail

LOCAL_DIR=${1:-exports}
# default remote for exports
REMOTE=${RCLONE_REMOTE_EXPORT:-onedrive:ETL_clean}
# If VERSIONED=1 then copy the exports into a dated folder on remote instead of sync
VERSIONED=${VERSIONED:-0}

if [ ! -d "$LOCAL_DIR" ]; then
  echo "Erreur: dossier local '$LOCAL_DIR' introuvable. Exécutez d'abord l'export DWH."
  exit 2
fi

if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone introuvable. Installez rclone (https://rclone.org/install/) puis configurez un remote OneDrive/SharePoint."
  exit 3
fi

if [ "$VERSIONED" -eq 1 ]; then
  today=$(date +%Y%m%d)
  target="$REMOTE/$today"
  echo "Upload versionné des exports: ${LOCAL_DIR} -> ${target} (copy)"
  # copy preserves older folders; use copy to avoid deleting remote history
  rclone copy "$LOCAL_DIR" "$target" --progress
  if [ $? -eq 0 ]; then
    echo "Upload versionné des exports terminé."
  else
    echo "Erreur lors de l'upload des exports versionnés."
    exit 4
  fi
else
  echo "Synchronisation: ${LOCAL_DIR} -> ${REMOTE}"
  rclone sync "$LOCAL_DIR" "$REMOTE" --progress
  if [ $? -eq 0 ]; then
    echo "Upload des exports vers OneDrive/SharePoint terminé."
  else
    echo "Erreur lors de l'upload des exports. Vérifiez la configuration rclone et les permissions."
    exit 4
  fi
fi
