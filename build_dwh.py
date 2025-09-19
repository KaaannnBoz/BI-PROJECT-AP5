import os
import psycopg

DATABASE_URL = "postgresql://admin:password@localhost:5432/BIPostgres"
if not DATABASE_URL:
    raise SystemExit("❌ DATABASE_URL non défini.")

with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
    with conn.cursor() as cur:
        # Création schéma
        cur.execute("CREATE SCHEMA IF NOT EXISTS dwh;")

        # Dimensions
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwh.dim_etudiant (
            etudiant_id BIGSERIAL PRIMARY KEY,
            nom TEXT, prenom TEXT, date_naissance DATE,
            nationalite TEXT, ecole TEXT,
            CONSTRAINT uq_etudiant UNIQUE (nom, prenom, date_naissance)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwh.dim_entreprise (
            entreprise_id BIGSERIAL PRIMARY KEY,
            entreprise TEXT NOT NULL, pays TEXT,
            CONSTRAINT uq_entreprise UNIQUE (entreprise, pays)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwh.dim_matiere (
            matiere_id BIGSERIAL PRIMARY KEY,
            libelle TEXT NOT NULL UNIQUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwh.dim_annee (
            annee_key INT PRIMARY KEY, libelle TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwh.dim_date (
            date_key INT PRIMARY KEY, full_date DATE NOT NULL,
            annee INT, mois INT, jour INT
        );
        """)

        # Table de faits
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwh.fact_parcours (
            fact_id BIGSERIAL PRIMARY KEY,
            etudiant_id BIGINT REFERENCES dwh.dim_etudiant(etudiant_id),
            annee_key INT REFERENCES dwh.dim_annee(annee_key),
            entreprise_id BIGINT REFERENCES dwh.dim_entreprise(entreprise_id),
            publie BOOLEAN, stage_duree_j INT,
            date_embauche_key INT REFERENCES dwh.dim_date(date_key),
            stage_debut_key INT REFERENCES dwh.dim_date(date_key),
            stage_fin_key INT REFERENCES dwh.dim_date(date_key)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwh.bridge_parcours_matiere (
            fact_id BIGINT REFERENCES dwh.fact_parcours(fact_id) ON DELETE CASCADE,
            matiere_id BIGINT REFERENCES dwh.dim_matiere(matiere_id),
            PRIMARY KEY (fact_id, matiere_id)
        );
        """)

        # Alimentation dimensions
        cur.execute("TRUNCATE dwh.fact_parcours CASCADE;")
        cur.execute("INSERT INTO dwh.dim_annee (annee_key, libelle) "
                    "SELECT DISTINCT annee, annee::text FROM ods.etudiants_clean "
                    "WHERE annee IS NOT NULL ON CONFLICT DO NOTHING;")

        cur.execute("""
        WITH d AS (
            SELECT date_embauche d FROM ods.etudiants_clean WHERE date_embauche IS NOT NULL
            UNION SELECT stage_debut FROM ods.etudiants_clean WHERE stage_debut IS NOT NULL
            UNION SELECT stage_fin FROM ods.etudiants_clean WHERE stage_fin IS NOT NULL
        )
        INSERT INTO dwh.dim_date(date_key, full_date, annee, mois, jour)
        SELECT DISTINCT EXTRACT(YEAR FROM d)::int*10000 + EXTRACT(MONTH FROM d)::int*100 + EXTRACT(DAY FROM d)::int,
               d::date, EXTRACT(YEAR FROM d)::int, EXTRACT(MONTH FROM d)::int, EXTRACT(DAY FROM d)::int
        FROM d ON CONFLICT (date_key) DO NOTHING;
        """)

        cur.execute("""
        INSERT INTO dwh.dim_etudiant (nom, prenom, date_naissance, nationalite, ecole)
        SELECT DISTINCT nom, prenom, date_naissance, nationalite, ecole
        FROM ods.etudiants_clean
        ON CONFLICT (nom, prenom, date_naissance)
        DO UPDATE SET nationalite = EXCLUDED.nationalite, ecole = EXCLUDED.ecole;
        """)

        cur.execute("""
        INSERT INTO dwh.dim_entreprise (entreprise, pays)
        SELECT DISTINCT stage_entreprise, stage_pays
        FROM ods.etudiants_clean WHERE stage_entreprise IS NOT NULL
        ON CONFLICT (entreprise, pays) DO NOTHING;
        """)

        cur.execute("""
        WITH split AS (
            SELECT DISTINCT trim(m) lib
            FROM ods.etudiants_clean,
            LATERAL regexp_split_to_table(COALESCE(matiere,''), '\\s*;\\s*') m
            WHERE COALESCE(matiere,'') <> ''
        )
        INSERT INTO dwh.dim_matiere(libelle)
        SELECT lib FROM split ON CONFLICT (libelle) DO NOTHING;
        """)

        # Table de faits
        cur.execute("""
        INSERT INTO dwh.fact_parcours (etudiant_id, annee_key, entreprise_id, publie, stage_duree_j,
                                       date_embauche_key, stage_debut_key, stage_fin_key)
        SELECT
            de.etudiant_id, o.annee, den.entreprise_id, o.publie, o.stage_duree_j,
            CASE WHEN o.date_embauche IS NOT NULL
                 THEN (EXTRACT(YEAR FROM o.date_embauche)::int*10000
                       + EXTRACT(MONTH FROM o.date_embauche)::int*100
                       + EXTRACT(DAY FROM o.date_embauche)::int) END,
            CASE WHEN o.stage_debut IS NOT NULL
                 THEN (EXTRACT(YEAR FROM o.stage_debut)::int*10000
                       + EXTRACT(MONTH FROM o.stage_debut)::int*100
                       + EXTRACT(DAY FROM o.stage_debut)::int) END,
            CASE WHEN o.stage_fin IS NOT NULL
                 THEN (EXTRACT(YEAR FROM o.stage_fin)::int*10000
                       + EXTRACT(MONTH FROM o.stage_fin)::int*100
                       + EXTRACT(DAY FROM o.stage_fin)::int) END
        FROM ods.etudiants_clean o
        JOIN dwh.dim_etudiant de ON de.nom=o.nom AND de.prenom=o.prenom
           AND (de.date_naissance IS NOT DISTINCT FROM o.date_naissance)
        JOIN dwh.dim_annee da ON da.annee_key=o.annee
        LEFT JOIN dwh.dim_entreprise den ON den.entreprise=o.stage_entreprise
           AND (den.pays IS NOT DISTINCT FROM o.stage_pays);
        """)

        # Bridge matières
        cur.execute("""
        WITH f AS (
            SELECT fp.fact_id, oc.matiere
            FROM dwh.fact_parcours fp
            JOIN dwh.dim_etudiant de ON de.etudiant_id=fp.etudiant_id
            JOIN dwh.dim_annee da ON da.annee_key=fp.annee_key
            JOIN ods.etudiants_clean oc
              ON oc.nom=de.nom AND oc.prenom=de.prenom
             AND (oc.date_naissance IS NOT DISTINCT FROM de.date_naissance)
             AND oc.annee=da.annee_key
        ),
        split AS (
            SELECT fact_id, trim(x) lib
            FROM f, LATERAL regexp_split_to_table(COALESCE(matiere,''), '\\s*;\\s*') x
            WHERE COALESCE(matiere,'') <> ''
        )
        INSERT INTO dwh.bridge_parcours_matiere(fact_id, matiere_id)
        SELECT s.fact_id, dm.matiere_id
        FROM split s
        JOIN dwh.dim_matiere dm ON dm.libelle=s.lib
        ON CONFLICT DO NOTHING;
        """)

        conn.commit()
        print("✅ DWH construit")
