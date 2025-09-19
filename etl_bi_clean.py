
import os, re, unicodedata, json
import pandas as pd
from datetime import datetime

# ========= Paramètres =========
SRC = "source_bruit_1000_final.xlsx"   # chemin du fichier source
OUT_DIR = "clean"                      # dossier de sortie
os.makedirs(OUT_DIR, exist_ok=True)

# ========= Utilitaires =========
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
    # Particules FR
    return re.sub(r"\b(D|L|De|Du|Des|La|Le|Les|D')\b", lambda m: m.group(0).lower(), x)

# Booléen robuste (1/0, 1.0/0.0, oui/non, true/false, ✓/✗, etc.)
def to_bool(x):
    if pd.isna(x): return None
    if isinstance(x, bool): return x
    if isinstance(x, (int,)): return bool(x)
    if isinstance(x, (float,)) and not pd.isna(x):
        try: return bool(int(round(x)))
        except Exception: pass
    s = str(x).strip().lower()
    if re.fullmatch(r"1(\.0+)?", s): return True
    if re.fullmatch(r"0(\.0+)?", s): return False
    truthy = {"true","vrai","oui","o","yes","y","1","t","x","✓","✔","publié","published"}
    falsy  = {"false","faux","non","n","no","0","f","✗","×","non publié","unpublished"}
    if s in truthy: return True
    if s in falsy:  return False
    return None

def parse_date(x):
    if pd.isna(x) or x == "": return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)): return pd.to_datetime(x)
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%m/%d/%Y","%Y/%m/%d"):
        try: return pd.to_datetime(datetime.strptime(str(x), fmt))
        except: pass
    return pd.to_datetime(x, dayfirst=True, errors="coerce")

def coalesce(*vals):
    for v in vals:
        if pd.notna(v) and v not in ("", None): return v
    return None

def agg_bool(s):
    # OR logique : True si au moins un vrai ; None si tout manquant
    if s.isna().all(): return None
    return bool(s.fillna(False).any())

def agg_text_longest(s):
    vals = [x for x in s.dropna().astype(str) if x.strip()!=""]
    return None if not vals else max(vals, key=len)

def agg_list_unique(series, sep="; "):
    vals = sorted(set([v.strip() for v in series.dropna().astype(str) if v.strip()]))
    return sep.join(vals) if vals else None

# ========= Lecture =========
rename_map = {
    "Nom":"nom","Prénom":"prenom","Date_Naissance":"date_naissance","Nationalité":"nationalite",
    "École":"ecole","Matière":"matiere","Année":"annee","Projet":"projet","Description_Projet":"description_projet",
    "Publié":"publie","Entreprise":"entreprise","Pays_Entreprise":"pays_entreprise","Date_Embauche":"date_embauche",
    "Stage_Entreprise":"stage_entreprise","Stage_Pays":"stage_pays","Stage_Début":"stage_debut","Stage_Fin":"stage_fin"
}
df = pd.read_excel(SRC, sheet_name=0).rename(columns=rename_map)

# ========= Nettoyage de base =========
for col in ["nom","prenom","nationalite","ecole","matiere","projet","description_projet",
            "entreprise","pays_entreprise","stage_entreprise","stage_pays"]:
    if col in df: df[col] = df[col].apply(clean_text)

df["nom"] = df["nom"].apply(proper_case_name)
df["prenom"] = df["prenom"].apply(proper_case_name)
df["annee"] = pd.to_numeric(df.get("annee"), errors="coerce").astype("Int64")

if "publie" in df:
    df["publie"] = df["publie"].apply(to_bool).astype("boolean")

for col in ["date_naissance","date_embauche","stage_debut","stage_fin"]:
    if col in df: df[col] = df[col].apply(parse_date)

mask = df["stage_fin"].notna() & df["stage_debut"].notna() & (df["stage_fin"] < df["stage_debut"])
df.loc[mask, ["stage_debut","stage_fin"]] = df.loc[mask, ["stage_fin","stage_debut"]].values


# Remplir stage_entreprise si vide avec entreprise
df["stage_entreprise"] = df.apply(lambda r: coalesce(r.get("stage_entreprise"), r.get("entreprise")), axis=1)

# ========= Dédup & agrégation (Personne × Année) =========
df["_key_year"] = df.apply(lambda r: "|".join([
    strip_accents_lower(coalesce(r.get("nom",""))),
    strip_accents_lower(coalesce(r.get("prenom",""))),
    str(pd.to_datetime(r["date_naissance"]).date() if pd.notna(r.get("date_naissance")) else ""),
    str(r["annee"]) if pd.notna(r.get("annee")) else ""
]), axis=1)

agg_dict_year = {
    "nom":"first",
    "prenom":"first",
    "date_naissance":"first",
    "annee":"first",
    "nationalite":"first",
    "ecole":"first",
    "matiere": lambda s: agg_list_unique(s, "; "),  # matières regroupées uniques
    "projet":"first",
    "description_projet":agg_text_longest,
    "publie":agg_bool,                    # OR logique
    "entreprise":"first",
    "pays_entreprise":"first",
    "date_embauche":"max",
    "stage_entreprise":"first",
    "stage_pays":"first",
    "stage_debut":"min",
    "stage_fin":"max",
}
clean = df.groupby("_key_year", dropna=False).agg(agg_dict_year).reset_index(drop=True)

# ========= Post-traitements =========

if "publie" in clean.columns:
    clean["publie"] = clean["publie"].map({True: "True", False: "False"}).fillna("NULL")

date_cols = ["date_naissance","date_embauche","stage_debut","stage_fin"]
for c in date_cols:
    if c in clean.columns:
        clean[c] = pd.to_datetime(clean[c], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")

# ========= Exports =========
out_csv  = os.path.join(OUT_DIR, "source_bruit_1000_final_clean_annee.csv")
out_xlsx = os.path.join(OUT_DIR, "source_bruit_1000_final_clean_annee.xlsx")
report_json = os.path.join(OUT_DIR, "data_quality_report.json")

clean.to_csv(out_csv, index=False, encoding="utf-8")
with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as w:
    clean.to_excel(w, index=False, sheet_name="clean_by_year")

# Petit rapport de contrôle
dq = {
    "nb_lignes_sortie": len(clean),
    "compte_publie": clean["publie"].value_counts(dropna=False).to_dict(),
    "dates_vides": {c:int((clean[c] == "").sum()) for c in date_cols},
}
with open(report_json, "w", encoding="utf-8") as f:
    json.dump(dq, f, ensure_ascii=False, indent=2)

print("Nettoyage terminés la team")
print("→", out_csv)
print("→", out_xlsx)
print("→", report_json)
