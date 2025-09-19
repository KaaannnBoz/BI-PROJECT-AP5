import psycopg

DATABASE_URL = "postgresql://admin:password@localhost:5432/BIPostgres"

DDL = """
CREATE SCHEMA IF NOT EXISTS dwh;

-- drop & recreate (propre)
DROP TABLE IF EXISTS dwh.fait_annee CASCADE;
DROP TABLE IF EXISTS dwh.dimension_info_stage CASCADE;
DROP TABLE IF EXISTS dwh.dimension_projet CASCADE;
DROP TABLE IF EXISTS dwh.dimension_matiere CASCADE;
DROP TABLE IF EXISTS dwh.dimension_ecole CASCADE;
DROP TABLE IF EXISTS dwh.dimension_etudiant CASCADE;
DROP TABLE IF EXISTS dwh.dimension_employe CASCADE;

CREATE TABLE dwh.dimension_employe (
  id_employe     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  date_embauche  DATE,
  entreprise     TEXT,
  pays           TEXT
);

CREATE TABLE dwh.dimension_etudiant (
  id_etudiant     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nom             TEXT,
  prenom          TEXT,
  date_naissance  DATE,
  nationalite     TEXT,
  id_employe      BIGINT REFERENCES dwh.dimension_employe(id_employe)
);

CREATE TABLE dwh.dimension_ecole (
  id_ecole   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nom_ecole  TEXT UNIQUE
);

CREATE TABLE dwh.dimension_matiere (
  id_matiere   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nom_matiere  TEXT UNIQUE
);

CREATE TABLE dwh.dimension_projet (
  id_projet    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nom_projet   TEXT,
  description  TEXT,
  publier      BOOLEAN
);

CREATE TABLE dwh.dimension_info_stage (
  id_stage     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  pays         TEXT,
  entreprise   TEXT,
  date_debut   DATE,
  date_fin     DATE
);

CREATE TABLE dwh.fait_annee (
  id_template  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  annee        INT NOT NULL,
  id_ecole     BIGINT REFERENCES dwh.dimension_ecole(id_ecole),
  id_stage     BIGINT REFERENCES dwh.dimension_info_stage(id_stage),
  id_etudiant  BIGINT REFERENCES dwh.dimension_etudiant(id_etudiant),
  id_projet    BIGINT REFERENCES dwh.dimension_projet(id_projet),
  id_matiere   BIGINT REFERENCES dwh.dimension_matiere(id_matiere)
);

-- petits index utiles pour Power BI
CREATE INDEX ON dwh.fait_annee (annee);
CREATE INDEX ON dwh.fait_annee (id_etudiant);
CREATE INDEX ON dwh.fait_annee (id_matiere);
"""

STEPS = [

# Employé (embauche + entreprise/pays)
("""
INSERT INTO dwh.dimension_employe (date_embauche, entreprise, pays)
SELECT DISTINCT
  o.date_embauche, NULLIF(o.entreprise,''), NULLIF(o.pays_entreprise,'')
FROM ods.etudiants_clean o
WHERE o.date_embauche IS NOT NULL
   OR NULLIF(o.entreprise,'') IS NOT NULL
   OR NULLIF(o.pays_entreprise,'') IS NOT NULL;
""","dimension_employe"),

# Étudiant (+ FK employé si retrouvable)
("""
INSERT INTO dwh.dimension_etudiant (nom, prenom, date_naissance, nationalite, id_employe)
SELECT DISTINCT
  o.nom, o.prenom, o.date_naissance, o.nationalite,
  e.id_employe
FROM ods.etudiants_clean o
LEFT JOIN dwh.dimension_employe e
  ON e.entreprise = NULLIF(o.entreprise,'')
 AND e.pays       = NULLIF(o.pays_entreprise,'')
 AND (e.date_embauche IS NOT DISTINCT FROM o.date_embauche);
""","dimension_etudiant"),

# École
("""
INSERT INTO dwh.dimension_ecole (nom_ecole)
SELECT DISTINCT NULLIF(o.ecole,'')
FROM ods.etudiants_clean o
WHERE NULLIF(o.ecole,'') IS NOT NULL
ON CONFLICT (nom_ecole) DO NOTHING;
""","dimension_ecole"),

# Matière (explode)
("""
WITH mat AS (
  SELECT DISTINCT TRIM(m) AS nom_matiere
  FROM ods.etudiants_clean o,
       LATERAL regexp_split_to_table(COALESCE(o.matiere,''), '\\s*;\\s*') AS m
)
INSERT INTO dwh.dimension_matiere (nom_matiere)
SELECT nom_matiere FROM mat
WHERE nom_matiere <> ''
ON CONFLICT (nom_matiere) DO NOTHING;
""","dimension_matiere"),

# Projet
("""
INSERT INTO dwh.dimension_projet (nom_projet, description, publier)
SELECT DISTINCT
  NULLIF(o.projet,''), NULLIF(o.description_projet,''), o.publie
FROM ods.etudiants_clean o;
""","dimension_projet"),

# Info stage
("""
INSERT INTO dwh.dimension_info_stage (pays, entreprise, date_debut, date_fin)
SELECT DISTINCT
  NULLIF(o.stage_pays,''), NULLIF(o.stage_entreprise,''), o.stage_debut, o.stage_fin
FROM ods.etudiants_clean o
WHERE o.stage_debut IS NOT NULL
   OR o.stage_fin   IS NOT NULL
   OR NULLIF(o.stage_pays,'') IS NOT NULL
   OR NULLIF(o.stage_entreprise,'') IS NOT NULL;
""","dimension_info_stage"),

# Fait année (1 ligne par matière)
("""
INSERT INTO dwh.fait_annee (annee, id_ecole, id_stage, id_etudiant, id_projet, id_matiere)
SELECT
  o.annee,
  ec.id_ecole,
  st.id_stage,
  et.id_etudiant,
  pr.id_projet,
  ma.id_matiere
FROM ods.etudiants_clean o
LEFT JOIN dwh.dimension_etudiant et
  ON et.nom=o.nom AND et.prenom=o.prenom
 AND (et.date_naissance IS NOT DISTINCT FROM o.date_naissance)
LEFT JOIN dwh.dimension_ecole ec
  ON ec.nom_ecole = NULLIF(o.ecole,'')
LEFT JOIN dwh.dimension_projet pr
  ON pr.nom_projet  IS NOT DISTINCT FROM NULLIF(o.projet,'')
 AND pr.description IS NOT DISTINCT FROM NULLIF(o.description_projet,'')
 AND pr.publier     IS NOT DISTINCT FROM o.publie
LEFT JOIN dwh.dimension_info_stage st
  ON st.entreprise  IS NOT DISTINCT FROM NULLIF(o.stage_entreprise,'')
 AND st.pays        IS NOT DISTINCT FROM NULLIF(o.stage_pays,'')
 AND st.date_debut  IS NOT DISTINCT FROM o.stage_debut
 AND st.date_fin    IS NOT DISTINCT FROM o.stage_fin
LEFT JOIN LATERAL (
    SELECT TRIM(x) AS nom_matiere
    FROM regexp_split_to_table(COALESCE(o.matiere,''), '\\s*;\\s*') x
) m ON TRUE
LEFT JOIN dwh.dimension_matiere ma
  ON ma.nom_matiere = NULLIF(m.nom_matiere,'')
WHERE o.annee IS NOT NULL;
""","fait_annee"),
]

def main():
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        cur = conn.cursor()
        print("(Re)création du DWH…")
        cur.execute(DDL)

        for sql, name in STEPS:
            print(f"➡️  Charge {name} …")
            cur.execute(sql)
            cur.execute(f"SELECT COUNT(*) FROM dwh.{name};")
            print(f" {name}: {cur.fetchone()[0]} lignes")

        print("DWH construit (modèle = diagramme).")

if __name__ == "__main__":
    main()
