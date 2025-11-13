import os, sys, csv
from pathlib import Path
import psycopg

# Load .env if present (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://admin:password@localhost:5432/BIPostgres")
CSV_RELATIVE = os.environ.get("CSV_RELATIVE", "clean/source_bruit_1000_final_clean_annee.csv")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))

COLS = [
    "nom","prenom","date_naissance","annee","nationalite","ecole","matiere",
    "projet","description_projet","publie","entreprise","pays_entreprise",
    "date_embauche","stage_entreprise","stage_pays","stage_debut","stage_fin"
]
INT_COLS  = {"annee"}
BOOL_COLS = {"publie"}

def norm_empty(v):
    """'' ou 'NULL' -> None ; sinon string strip()"""
    if v is None: return None
    s = str(v).strip()
    if s == "" or s.upper() == "NULL": return None
    return s

def norm_bool(v):
    v = norm_empty(v)
    if v is None: return None
    if v == "True": return True
    if v == "False": return False
    return None

def main():
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL manquant.")

    csv_path = Path(sys.argv[1] if len(sys.argv) > 1 else CSV_RELATIVE).resolve()
    if not csv_path.exists():
        raise SystemExit(f"CSV introuvable : {csv_path}")

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        num_lines = sum(1 for _ in f)
    if num_lines <= 1:
        raise SystemExit(f"Le CSV ne contient pas de données (wc -l = {num_lines}).")

    print(f"CSV : {csv_path}  (~{num_lines-1} lignes)")
    print(f"DB  : {DATABASE_URL}")

    DDL = """
    CREATE SCHEMA IF NOT EXISTS ods;

    DROP TABLE IF EXISTS ods.etudiants_clean;

    CREATE TABLE ods.etudiants_clean (
      nom                TEXT,
      prenom             TEXT,
      date_naissance     DATE,
      annee              INT,
      nationalite        TEXT,
      ecole              TEXT,
      matiere            TEXT,
      projet             TEXT,
      description_projet TEXT,
      publie             BOOLEAN,
      entreprise         TEXT,
      pays_entreprise    TEXT,
      date_embauche      DATE,
      stage_entreprise   TEXT,
      stage_pays         TEXT,
      stage_debut        DATE,
      stage_fin          DATE
    );
    """

    insert_sql = f"""
      INSERT INTO ods.etudiants_clean ({", ".join(COLS)})
      VALUES ({", ".join(["%s"] * len(COLS))})
    """

    inserted = 0
    batch = []

    with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
        with conn.cursor() as cur:
            print("🧱 (Re)création du schéma & table ODS…")
            cur.execute(DDL)

            print("📥 Insertion par batch…")
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                missing = [c for c in COLS if c not in reader.fieldnames]
                if missing:
                    raise SystemExit(f"❌ Colonnes manquantes dans le CSV: {missing}")

                for row in reader:
                    vals = []
                    for c in COLS:
                        if c in BOOL_COLS:
                            vals.append(norm_bool(row.get(c)))
                        else:
                            vals.append(norm_empty(row.get(c)))
                    batch.append(vals)

                    if len(batch) >= BATCH_SIZE:
                        cur.executemany(insert_sql, batch)
                        inserted += len(batch)
                        batch.clear()

                if batch:
                    cur.executemany(insert_sql, batch)
                    inserted += len(batch)
                    batch.clear()

            conn.commit()

    print(f"ODS chargé : {inserted} ligne(s) dans ods.etudiants_clean")

if __name__ == "__main__":
    main()
