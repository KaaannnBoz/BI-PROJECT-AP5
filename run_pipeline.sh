#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Chargement auto du fichier .env si présent
# -----------------------------
if [ -f ".env" ]; then
  echo "Chargement des variables depuis .env"
  set -o allexport
  source .env
  set +o allexport
fi

# -----------------------------
# Fallback: valeurs par défaut si non définies
# -----------------------------
DATABASE_URL="${DATABASE_URL:-postgresql://admin:password@localhost:5432/BIPostgres}"
RCLONE_REMOTE_EXPORT="${RCLONE_REMOTE_EXPORT:-sharepoint_bi:General/PowerBi/csv}"

export DATABASE_URL
export RCLONE_REMOTE_EXPORT

echo "DATABASE_URL = $DATABASE_URL"
echo "RCLONE_REMOTE_EXPORT = $RCLONE_REMOTE_EXPORT"

# -----------------------------
# Source input
# -----------------------------
SRC=${1:-source_bruit_1000_final.xlsx}

echo "1) Nettoyage -> dossier clean/"
python3 etl_bi_clean.py "$SRC" --out clean

if [ -z "${DATABASE_URL}" ]; then
  echo "WARNING: DATABASE_URL vide. ODS, DWH et export CSV seront sautés."
else
  echo "2) Chargement ODS -> Postgres"
  python3 etl_to_ods.py clean/source_bruit_1000_final_clean_annee.csv --batch 1000

  echo "3) Construction DWH -> Postgres"
  python3 build_dwh.py

  echo "4) Export DWH -> CSV (exports/)"
  python3 export_dwh_to_csv.py
fi

echo "5) Upload exports -> $RCLONE_REMOTE_EXPORT"
./upload_exports_to_sharepoint.sh || {
  echo "Erreur upload exports"
  exit 5
}

echo "Pipeline terminé"
