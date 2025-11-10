# build_dwh.py
import psycopg

DATABASE_URL = "postgresql://admin:password@localhost:5432/BIPostgres"

DDL = """
CREATE SCHEMA IF NOT EXISTS dwh;

-- drop & recreate
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

-- IMPORTANT: on garde la structure (2 colonnes) mais SANS contrainte UNIQUE
CREATE TABLE dwh.dimension_matiere (
  id_matiere   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nom_matiere  TEXT
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

CREATE INDEX ON dwh.fait_annee (annee);
CREATE INDEX ON dwh.fait_annee (id_etudiant);
"""

STEPS_SQL = [
# Employé
("""
INSERT INTO dwh.dimension_employe (date_embauche, entreprise, pays)
SELECT DISTINCT
  o.date_embauche, NULLIF(o.entreprise,''), NULLIF(o.pays_entreprise,'')
FROM ods.etudiants_clean o
WHERE o.date_embauche IS NOT NULL
   OR NULLIF(o.entreprise,'') IS NOT NULL
   OR NULLIF(o.pays_entreprise,'') IS NOT NULL;
""","dimension_employe"),

# Étudiant
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

# Projet
("""
INSERT INTO dwh.dimension_projet (nom_projet, description, publier)
SELECT DISTINCT
  NULLIF(o.projet,''), NULLIF(o.description_projet,''), o.publie
FROM ods.etudiants_clean o;
""","dimension_projet"),

# Info stage (on exclut les lignes "entreprise seule")
("""
INSERT INTO dwh.dimension_info_stage (pays, entreprise, date_debut, date_fin)
WITH base AS (
  SELECT DISTINCT
    NULLIF(NULLIF(NULLIF(TRIM(o.stage_pays),'NULL'),'null'),'')      AS pays,
    NULLIF(NULLIF(NULLIF(TRIM(o.stage_entreprise),'NULL'),'null'),'') AS entreprise,
    o.stage_debut::date AS date_debut,
    o.stage_fin::date   AS date_fin
  FROM ods.etudiants_clean o
),
filtered AS (
  -- on garde seulement si l'entreprise existe ET (pays OU dates) existent
  SELECT *
  FROM base
  WHERE entreprise IS NOT NULL
    AND (pays IS NOT NULL OR date_debut IS NOT NULL OR date_fin IS NOT NULL)
)
SELECT pays, entreprise, date_debut, date_fin
FROM filtered;
""","dimension_info_stage"),
]

def main():
    # autocommit=True pour simplifier, la TEMP table PRESERVE ROWS gardera ses lignes pendant la session
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        cur = conn.cursor()
        print("🧱 (Re)création du DWH…")
        cur.execute(DDL)

        # Étapes SQL "simples"
        for sql, name in STEPS_SQL:
            print(f"➡️  Charge {name} …")
            cur.execute(sql)
            cur.execute(f"SELECT COUNT(*) FROM dwh.{name};")
            print(f"   ✅ {name}: {cur.fetchone()[0]} lignes")

        # 1) Construire (id_etudiant, annee, liste_matieres)
        print("🧩 Agrégation matières par (étudiant, année)…")
        cur.execute("""
            WITH b AS (
              SELECT
                de.id_etudiant,
                o.annee,
                TRIM(x) AS mat
              FROM ods.etudiants_clean o
              JOIN dwh.dimension_etudiant de
                ON de.nom=o.nom AND de.prenom=o.prenom
               AND (de.date_naissance IS NOT DISTINCT FROM o.date_naissance)
              CROSS JOIN LATERAL regexp_split_to_table(COALESCE(o.matiere,''), '\\s*;\\s*') x
              WHERE o.annee IS NOT NULL
                AND COALESCE(x,'') <> ''
            ),
            agg AS (
              SELECT
                id_etudiant,
                annee,
                ARRAY(
                  SELECT DISTINCT m2.mat
                  FROM b m2
                  WHERE m2.id_etudiant=b.id_etudiant AND m2.annee=b.annee
                  ORDER BY m2.mat
                ) AS mats
              FROM b
              GROUP BY id_etudiant, annee
            )
            SELECT id_etudiant, annee, array_to_string(mats, '; ') AS matieres_text
            FROM agg
            ORDER BY id_etudiant, annee;
        """)
        rows = cur.fetchall()  # [(id_etudiant, annee, matieres_text), ...]

        # 2) Insérer dans dimension_matiere (1 ligne par élève-année) et mémoriser la correspondance
        print("🧾 Insertion dimension_matiere (élève-année)…")
        mapping = []  # (id_etudiant, annee, id_matiere)
        for (id_etudiant, annee, mat_text) in rows:
            cur.execute(
                "INSERT INTO dwh.dimension_matiere (nom_matiere) VALUES (%s) RETURNING id_matiere;",
                (mat_text if mat_text else None,)
            )
            id_matiere = cur.fetchone()[0]
            mapping.append((id_etudiant, annee, id_matiere))

        # 3) Staging mapping pour joindre proprement (TEMP sans schéma + PRESERVE ROWS)
        cur.execute("DROP TABLE IF EXISTS _map_matiere;")
        cur.execute("""
            CREATE TEMP TABLE _map_matiere(
                id_etudiant BIGINT,
                annee INT,
                id_matiere BIGINT
            ) ON COMMIT PRESERVE ROWS;
        """)

        cur.executemany(
            "INSERT INTO _map_matiere(id_etudiant, annee, id_matiere) VALUES (%s,%s,%s);",
            mapping
        )

        # 4) Alimenter la table de faits (1 ligne par élève-année ; pas d’explosion par matière)
        print("📦 Insertion fait_annee…")
        cur.execute("""
            INSERT INTO dwh.fait_annee (annee, id_ecole, id_stage, id_etudiant, id_projet, id_matiere)
            SELECT
              o.annee,
              ec.id_ecole,
              st.id_stage,
              et.id_etudiant,
              pr.id_projet,
              mp.id_matiere
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
            JOIN _map_matiere mp
              ON mp.id_etudiant = et.id_etudiant
             AND mp.annee       = o.annee
            WHERE o.annee IS NOT NULL
            GROUP BY o.annee, ec.id_ecole, st.id_stage, et.id_etudiant, pr.id_projet, mp.id_matiere;
        """)

        # Comptages
        for name in ["dimension_matiere","fait_annee"]:
            cur.execute(f"SELECT COUNT(*) FROM dwh.{name};")
            print(f"   ✅ {name}: {cur.fetchone()[0]} lignes")

        print("🎉 DWH reconstruit : id_matiere = élève-année ; nom_matiere = liste agrégée.")
        
if __name__ == "__main__":
    main()
