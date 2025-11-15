import os, re, unicodedata, json
import pandas as pd
from datetime import datetime
import argparse

def main(src: str = "source_bruit_1000_final.xlsx", out_dir: str = "clean"):
    os.makedirs(out_dir, exist_ok=True)

    # ========= Lecture =========
    rename_map = {
        "Nom":"nom","Prénom":"prenom","Date_Naissance":"date_naissance","Nationalité":"nationalite",
        "École":"ecole","Matière":"matiere","Année":"annee","Projet":"projet","Description_Projet":"description_projet",
        "Publié":"publie","Entreprise":"entreprise","Pays_Entreprise":"pays_entreprise","Date_Embauche":"date_embauche",
        "Stage_Entreprise":"stage_entreprise","Stage_Pays":"stage_pays","Stage_Début":"stage_debut","Stage_Fin":"stage_fin"
    }
    df = pd.read_excel(src, sheet_name=0).rename(columns=rename_map)

    # ========= Nettoyage basique =========
    text_cols = ["nom","prenom","nationalite","ecole","matiere","projet","description_projet",
                 "entreprise","pays_entreprise","stage_entreprise","stage_pays"]

    for col in text_cols:
        if col in df:
            df[col] = df[col].apply(clean_text)

    df["nom"] = df["nom"].apply(proper_case_name)
    df["prenom"] = df["prenom"].apply(proper_case_name)

    # Année
    df["annee"] = pd.to_numeric(df.get("annee"), errors="coerce").astype("Int64")

    # Booléen
    if "publie" in df:
        df["publie"] = df["publie"].apply(to_bool).astype("boolean")

    # Dates
    for col in ["date_naissance","date_embauche","stage_debut","stage_fin"]:
        if col in df:
            df[col] = df[col].apply(parse_date)

    # Corriger inversion stage
    mask = df["stage_fin"].notna() & df["stage_debut"].notna() & (df["stage_fin"] < df["stage_debut"])
    df.loc[mask, ["stage_debut","stage_fin"]] = df.loc[mask, ["stage_fin","stage_debut"]].values

    # Stage entreprise = entreprise si vide
    df["stage_entreprise"] = df.apply(
        lambda r: coalesce(r.get("stage_entreprise"), r.get("entreprise")),
        axis=1
    )

    # ========= Dédup par élève + année =========
    df["_key_year"] = df.apply(lambda r: "|".join([
        strip_accents_lower(coalesce(r.get("nom",""))),
        strip_accents_lower(coalesce(r.get("prenom",""))),
        str(pd.to_datetime(r["date_naissance"]).date() if pd.notna(r.get("date_naissance")) else ""),
        str(r["annee"]) if pd.notna(r.get("annee")) else ""
    ]), axis=1)

    agg_dict_year = {
        "nom":"first", "prenom":"first",
        "date_naissance":"first", "annee":"first",
        "nationalite":"first", "ecole":"first",
        "matiere": lambda s: agg_list_unique(s, "; "),
        "projet":"first", "description_projet": agg_text_longest,
        "publie": agg_bool,
        "entreprise":"first", "pays_entreprise":"first",
        "date_embauche":"first",
        "stage_entreprise":"first", "stage_pays":"first",
        "stage_debut":"first", "stage_fin":"first"
    }

    clean = df.groupby("_key_year", dropna=False).agg(agg_dict_year).reset_index(drop=True)

    # Publie → string
    if "publie" in clean:
        clean["publie"] = clean["publie"].map({True: "True", False: "False"}).fillna("NULL")

    # Dates format texte
    for c in ["date_naissance","date_embauche","stage_debut","stage_fin"]:
        clean[c] = pd.to_datetime(clean[c], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")

    # ========= Export =========
    out_csv = os.path.join(out_dir, "source_bruit_1000_final_clean_annee.csv")
    out_xlsx = os.path.join(out_dir, "source_bruit_1000_final_clean_annee.xlsx")
    report_json = os.path.join(out_dir, "data_quality_report.json")

    clean.to_csv(out_csv, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as w:
        clean.to_excel(w, index=False, sheet_name="clean_by_year")

    dq = {
        "nb_lignes_sortie": len(clean),
        "compte_publie": clean["publie"].value_counts(dropna=False).to_dict(),
    }

    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(dq, f, ensure_ascii=False, indent=2)

    print("Nettoyage terminé")
    print("→", out_csv)
    print("→", out_xlsx)
    print("→", report_json)

# ========== UTILITAIRES ==========
def normalize_spaces(s):
    return re.sub(r"\s+", " ", s.strip()) if isinstance(s, str) else s

def strip_accents_lower(s):
    if s is None or pd.isna(s): return ""
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c)).lower()

def clean_text(x):
    if pd.isna(x): return None
    return normalize_spaces(str(x))

def proper_case_name(x):
    if not isinstance(x, str): return x
    x = normalize_spaces(x.lower()).title()
    return re.sub(r"\b(D|L|De|Du|Des|La|Le|Les|D')\b", lambda m: m.group(0).lower(), x)

def to_bool(x):
    if pd.isna(x): return None
    s = str(x).strip().lower()
    if s in {"true","vrai","oui","1"}: return True
    if s in {"false","faux","non","0"}: return False
    return None

def parse_date(x):
    if pd.isna(x) or x == "": return pd.NaT
    return pd.to_datetime(x, dayfirst=True, errors="coerce")

def coalesce(*vals):
    for v in vals:
        if pd.notna(v) and v not in ("", None): return v
    return None

def agg_bool(s):
    if s.isna().all(): return None
    return bool(s.fillna(False).any())

def agg_text_longest(s):
    vals = [x for x in s.dropna().astype(str) if x.strip()!=""]
    return None if not vals else max(vals, key=len)

def agg_list_unique(series, sep="; "):
    vals = sorted(set(v.strip() for v in series.dropna().astype(str) if v.strip()))
    return sep.join(vals) if vals else None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("src", nargs="?", default="source_bruit_1000_final.xlsx")
    parser.add_argument("--out", default="clean")
    args = parser.parse_args()
    main(args.src, args.out)
