import os
import pandas as pd
import psycopg

DATABASE_URL = "postgresql://admin:password@localhost:5432/BIPostgres"
OUT_DIR = "exports"   # change si tu veux
SEP = ","             # mets ";" si tu préfères un CSV point-virgule

TABLES = [
    "dwh.dimension_employe",
    "dwh.dimension_etudiant",
    "dwh.dimension_ecole",
    "dwh.dimension_projet",
    "dwh.dimension_info_stage",
    "dwh.dimension_matiere",
    "dwh.fait_annee",
]

FLAT_SQL = """
SELECT
  f.id_template, f.annee,
  et.nom, et.prenom, et.date_naissance, et.nationalite,
  ec.nom_ecole,
  st.pays AS stage_pays, st.entreprise AS stage_entreprise, st.date_debut AS stage_debut, st.date_fin AS stage_fin,
  pr.nom_projet, pr.description, pr.publier,
  dm.nom_matiere AS matieres
FROM dwh.fait_annee f
LEFT JOIN dwh.dimension_etudiant    et ON et.id_etudiant=f.id_etudiant
LEFT JOIN dwh.dimension_ecole       ec ON ec.id_ecole=f.id_ecole
LEFT JOIN dwh.dimension_info_stage  st ON st.id_stage=f.id_stage
LEFT JOIN dwh.dimension_projet      pr ON pr.id_projet=f.id_projet
LEFT JOIN dwh.dimension_matiere     dm ON dm.id_matiere=f.id_matiere
ORDER BY et.id_etudiant, f.annee;
"""

def export_query(conn, sql, path):
    df = pd.read_sql_query(sql, conn)
    df.to_csv(path, index=False, sep=SEP, encoding="utf-8-sig")
    print(f"✅ {path} ({len(df)} lignes)")

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    with psycopg.connect(DATABASE_URL) as conn:
        # Tables brutes
        for t in TABLES:
            path = os.path.join(OUT_DIR, t.split(".")[-1] + ".csv")
            export_query(conn, f"SELECT * FROM {t};", path)

        # Vue aplatie
        export_query(conn, FLAT_SQL, os.path.join(OUT_DIR, "dwh_flat.csv"))

if __name__ == "__main__":
    main()
