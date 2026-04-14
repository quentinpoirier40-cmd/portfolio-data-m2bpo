"""
=============================================================================
ANALYSE DE CYCLICITE — v5
- Regle des 3 repetitions (periode max = n // 3)
- Periodes calendaires >= 0.40, insolites >= 0.80
- Une ligne par trimestre reel (axe X commun pour Looker)
- Upload BigQuery avec cast explicite
=============================================================================
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
from numpy.fft import rfft, rfftfreq
from statsmodels.tsa.seasonal import seasonal_decompose
from itertools import combinations
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

CREDENTIALS_PATH = '/home/quentin_poirier/scraper_emails/m2bpo-data-analyse-7daf9c338c7f.json'
PROJECT_ID       = 'm2bpo-data-analyse'
DATASET_TABLE    = 'm2bpo_data.10yrtenders_final_clean_M2BPO'
OUTPUT_TABLE     = 'm2bpo_data.cycles_profils_v5'
SEUIL_VOLUME     = 100
NIVEAU_MAX_COMBO = 3

# Seuils de fiabilite selon le type de periode
SEUIL_CALENDAIRE = 0.40   # periodes logiques : 6 mois, 1 an, 2 ans, 3 ans
SEUIL_INSOLITE   = 0.80   # periodes bizarres : 5 trim, 7 trim, 10 trim...

PERIODES_CALENDAIRES = [2, 4, 6, 8, 12]   # 6 mois / 1 an / 18 mois / 2 ans / 3 ans

NOMS_TRIM = {
    1: 'jan-mars',
    2: 'avr-juin',
    3: 'juil-sep',
    4: 'oct-dec',
}

PERIODE_LABELS = {
    2:  '6 mois',
    4:  '1 an',
    6:  '18 mois',
    8:  '2 ans',
    12: '3 ans',
}

# ── CONNEXION ─────────────────────────────────────────────────────────────────

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
print("Connecte a BigQuery")

# ── REQUETE PRE-AGREGEE ───────────────────────────────────────────────────────

print("Chargement depuis BigQuery...")

QUERY = f"""
SELECT
    DATE_TRUNC(CAST(inseree_le AS DATE), QUARTER) AS trimestre,
    Nature_cat_,
    Procedure_annonce,
    MO_type_,
    marche_cat,
    contrat_normalise,
    COUNT(*) AS nb_annonces
FROM `{DATASET_TABLE}`
WHERE inseree_le IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1
"""

df_raw = client.query(QUERY).to_dataframe()
df_raw['trimestre'] = pd.to_datetime(df_raw['trimestre'])
print(f"OK : {len(df_raw):,} lignes agregees | {df_raw['nb_annonces'].sum():,} annonces")
print(f"     Periode : {df_raw['trimestre'].min().date()} -> {df_raw['trimestre'].max().date()}")

# ── PARAMETRES ────────────────────────────────────────────────────────────────

COLS = ['Nature_cat_', 'Procedure_annonce', 'MO_type_', 'marche_cat', 'contrat_normalise']

LABELS = {
    'Nature_cat_':       'Nature (cat.)',
    'Procedure_annonce': 'Procedure',
    'MO_type_':          'MO (type)',
    'marche_cat':        'Marche (cat.)',
    'contrat_normalise': 'Contrat',
}

# ── FONCTIONS ─────────────────────────────────────────────────────────────────

def construire_serie(sub_df: pd.DataFrame) -> pd.Series:
    """Serie trimestrielle continue avec 0 pour les trimestres manquants."""
    return (
        sub_df.groupby('trimestre')['nb_annonces']
        .sum()
        .resample('QS')
        .sum()
        .fillna(0)
    )


def detecter_periode(ts: pd.Series) -> int | None:
    """
    FFT pour detecter la periode dominante.
    Plafond a n // 3 pour garantir au moins 3 repetitions du cycle.
    """
    n = len(ts)
    if n < 8:
        return None

    # Regle des 3 repetitions : periode max = n // 3
    periode_max = n // 3

    s = ts.values.astype(float)
    amplitudes = np.abs(rfft(s - s.mean()))
    frequences = rfftfreq(n)
    amplitudes[0] = 0

    idx  = np.argmax(amplitudes)
    freq = frequences[idx]
    if freq <= 0:
        return None

    periode = int(round(1 / freq))

    # Plafonnement a periode_max (regle des 3 repetitions)
    periode = max(2, min(periode, periode_max))

    return periode


def calculer_score(ts: pd.Series, periode: int) -> float:
    """Score fiabilite = Var(saisonniere) / [Var(saisonniere) + Var(residu)]"""
    try:
        decomp = seasonal_decompose(
            ts, model='additive', period=periode, extrapolate_trend='freq'
        )
        var_s = np.var(decomp.seasonal)
        var_r = np.var(decomp.resid.dropna())
        denom = var_s + var_r
        return round(float(var_s / denom) if denom > 0 else 0.0, 3)
    except Exception:
        return 0.0


def segment_valide(periode: int, score: float) -> bool:
    """
    Applique les seuils selon le type de periode :
    - Calendaire [2,4,6,8,12] : score >= 0.40
    - Insolite   [autres]     : score >= 0.80
    """
    if periode in PERIODES_CALENDAIRES:
        return score >= SEUIL_CALENDAIRE
    else:
        return score >= SEUIL_INSOLITE


def label_position(position_1based: int, periode: int, debut_trim: int) -> str:
    """
    Retourne le label calendaire d'une position dans le cycle.
    debut_trim : trimestre calendaire (1,2,3,4) ou demarre le cycle.

    Cycle 1 an  (4 trim) -> Q1 (jan-mars), Q2 (avr-juin)...
    Cycle 3 ans (12 trim)-> An1 - Q3 (juil-sep), An1 - Q4...
    """
    p = position_1based
    trim_reel = ((debut_trim + p - 2) % 4) + 1
    annee     = (debut_trim + p - 2) // 4 + 1
    trim_nom  = f"Q{trim_reel} ({NOMS_TRIM[trim_reel]})"

    if periode <= 4:
        return trim_nom
    else:
        return f"An{annee} - {trim_nom}"


def projeter_sur_historique(
    ts: pd.Series,
    decomp,
    periode: int,
    score: float,
    dimension: str,
    categorie: str,
    volume: int,
) -> pd.DataFrame:
    """
    Cree une ligne par trimestre reel de la serie historique.
    La valeur saisonniere de chaque trimestre = valeur de la position
    correspondante dans le cycle (profil moyen).

    Colonnes produites :
      dimension, categorie, volume_total, duree_cycle, nb_positions,
      nb_trimestres, score_fiabilite, diagnostic,
      trimestre (date), label_trimestre (ex: 2018-Q2),
      position_dans_cycle (1 a periode),
      label_calendaire (ex: Q2 (avr-juin) ou An2-Q4 (oct-dec)),
      amplitude_pct, direction, marqueur
    """
    sais   = decomp.seasonal
    n      = len(ts)

    # Profil moyen : valeur saisonniere par position dans le cycle (0-based)
    pos_0based = np.arange(n) % periode
    profil = (
        pd.Series(sais.values, index=pos_0based)
        .groupby(level=0)
        .mean()
    )
    # profil.index = 0, 1, 2, ... periode-1

    # Trimestre de depart du cycle = trimestre du creux + 1
    # On cherche le creux dans le profil
    pos_creux   = int(profil.idxmin())              # position 0-based du creux
    trim_creux  = (ts.index[pos_creux].quarter)     # trimestre calendaire du creux (1-4)
    debut_trim  = (trim_creux % 4) + 1              # trimestre de debut du cycle

    # Amplitude % par position (base = moyenne generale de la serie)
    moyenne = ts.mean() if ts.mean() != 0 else 1
    amplitude_profil = (profil / moyenne * 100).round(1)

    # Direction par position
    seuil_stable = profil.abs().mean() * 0.15
    directions = {}
    for i in range(periode):
        if i == 0:
            directions[i] = 'Depart'
        else:
            delta = profil[i] - profil[i - 1]
            if abs(delta) <= seuil_stable:
                directions[i] = 'Stable'
            elif delta > 0:
                directions[i] = 'Monte'
            else:
                directions[i] = 'Descend'

    # Marqueurs PIC et CREUX
    marqueurs = {i: '' for i in range(periode)}
    marqueurs[int(profil.idxmax())] = 'PIC'
    marqueurs[int(profil.idxmin())] = 'CREUX'

    # Diagnostic
    if score >= SEUIL_CALENDAIRE:
        diagnostic = 'Fiable'
    elif score >= 0.20:
        diagnostic = 'Partiel'
    else:
        diagnostic = 'Disparate'

    duree_label = PERIODE_LABELS.get(periode, f'{periode} trim.')

    # Construction des lignes : une par trimestre reel
    rows = []
    for i, date in enumerate(ts.index):
        pos  = i % periode                          # position 0-based dans le cycle
        p1   = pos + 1                              # position 1-based

        quarter_num   = date.quarter                # 1, 2, 3 ou 4
        annee_cal     = date.year
        label_trim    = f"{annee_cal}-Q{quarter_num}"

        rows.append({
            'dimension':          dimension,
            'categorie':          categorie,
            'volume_total':       volume,
            'duree_cycle':        duree_label,
            'nb_positions':       periode,
            'nb_trimestres':      n,
            'score_fiabilite':    score,
            'diagnostic':         diagnostic,
            'trimestre':          date,
            'label_trimestre':    label_trim,
            'position_dans_cycle': p1,
            'label_calendaire':   label_position(p1, periode, debut_trim),
            'amplitude_pct':      float(amplitude_profil[pos]),
            'direction':          directions[pos],
            'marqueur':           marqueurs[pos],
        })

    return pd.DataFrame(rows)


# ── BOUCLE PRINCIPALE ─────────────────────────────────────────────────────────

def traiter_combo(cols_combo: list, df: pd.DataFrame, desc: str) -> list:
    resultats = []

    volumes = (
        df.groupby(cols_combo)['nb_annonces']
        .sum()
        .reset_index()
        .rename(columns={'nb_annonces': 'volume'})
    )
    valides = volumes[volumes['volume'] >= SEUIL_VOLUME]

    for _, row in tqdm(valides.iterrows(), total=len(valides),
                       desc=f"{desc:<45}", unit='seg', ncols=100):

        mask = pd.Series(True, index=df.index)
        for col in cols_combo:
            mask &= (df[col] == row[col])

        ts      = construire_serie(df[mask])
        periode = detecter_periode(ts)
        if periode is None:
            continue

        score = calculer_score(ts, periode)

        # Filtre : calendaire >= 0.40 / insolite >= 0.80
        if not segment_valide(periode, score):
            continue

        # Decomposition pour projection sur historique
        try:
            decomp = seasonal_decompose(
                ts, model='additive', period=periode, extrapolate_trend='freq'
            )
        except Exception:
            continue

        cat_label = ' | '.join(str(row[c]) for c in cols_combo)
        dim_label = ' x '.join(LABELS[c] for c in cols_combo)

        df_proj = projeter_sur_historique(
            ts, decomp, periode, score, dim_label, cat_label, int(row['volume'])
        )
        if not df_proj.empty:
            resultats.append(df_proj)

    return resultats


# ── LANCEMENT ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("  ETAPE 1/2 - Dimensions simples")
print("=" * 70)

tous_profils = []

for col in COLS:
    res = traiter_combo([col], df_raw, LABELS[col])
    tous_profils.extend(res)
    print(f"  {LABELS[col]} : {len(res)} segments gardes")

print("\n" + "=" * 70)
print("  ETAPE 2/2 - Combinaisons (niveaux 2 et 3)")
print("=" * 70)

combos = []
for r in range(2, NIVEAU_MAX_COMBO + 1):
    for combo in combinations(COLS, r):
        combos.append(list(combo))

print(f"  {len(combos)} combinaisons\n")

for cols_combo in combos:
    label = ' x '.join(LABELS[c] for c in cols_combo)
    res = traiter_combo(cols_combo, df_raw, label)
    tous_profils.extend(res)

# ── TABLE FINALE ──────────────────────────────────────────────────────────────

if not tous_profils:
    print("Aucun segment valide trouve.")
else:
    table_finale = pd.concat(tous_profils, ignore_index=True)
    table_finale = table_finale.sort_values(
        ['score_fiabilite', 'dimension', 'categorie', 'trimestre'],
        ascending=[False, True, True, True]
    ).reset_index(drop=True)

    nb_seg = table_finale['categorie'].nunique()
    print(f"\nTable finale : {len(table_finale):,} lignes | {nb_seg} segments")
    print(f"({nb_seg} segments x ~40 trimestres)")

    print("\nRepartition par duree de cycle :")
    print(
        table_finale.drop_duplicates('categorie')
        .groupby('duree_cycle')['score_fiabilite']
        .agg(nb='count', score_moyen='mean')
        .round(3)
        .sort_values('nb', ascending=False)
        .to_string()
    )

    # ── EXPORT CSV ────────────────────────────────────────────────────────────

    CSV_PATH = 'cycles_profils_v5.csv'
    table_finale.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"\nExport CSV : {CSV_PATH}")

    # ── UPLOAD BIGQUERY ───────────────────────────────────────────────────────

    print(f"\nUpload vers BigQuery : {OUTPUT_TABLE} ...")

    # Cast explicite de tous les types pour eviter le bug Empty schema
    df_upload = table_finale.copy()
    df_upload['dimension']          = df_upload['dimension'].astype(str)
    df_upload['categorie']          = df_upload['categorie'].astype(str)
    df_upload['volume_total']       = df_upload['volume_total'].astype(int)
    df_upload['duree_cycle']        = df_upload['duree_cycle'].astype(str)
    df_upload['nb_positions']       = df_upload['nb_positions'].astype(int)
    df_upload['nb_trimestres']      = df_upload['nb_trimestres'].astype(int)
    df_upload['score_fiabilite']    = df_upload['score_fiabilite'].astype(float)
    df_upload['diagnostic']         = df_upload['diagnostic'].astype(str)
    df_upload['trimestre']          = pd.to_datetime(df_upload['trimestre'])
    df_upload['label_trimestre']    = df_upload['label_trimestre'].astype(str)
    df_upload['position_dans_cycle']= df_upload['position_dans_cycle'].astype(int)
    df_upload['label_calendaire']   = df_upload['label_calendaire'].fillna('').astype(str)
    df_upload['amplitude_pct']      = pd.to_numeric(df_upload['amplitude_pct'], errors='coerce')
    df_upload['direction']          = df_upload['direction'].fillna('').astype(str)
    df_upload['marqueur']           = df_upload['marqueur'].fillna('').astype(str)

    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        schema=[
            bigquery.SchemaField('dimension',           'STRING'),
            bigquery.SchemaField('categorie',           'STRING'),
            bigquery.SchemaField('volume_total',        'INTEGER'),
            bigquery.SchemaField('duree_cycle',         'STRING'),
            bigquery.SchemaField('nb_positions',        'INTEGER'),
            bigquery.SchemaField('nb_trimestres',       'INTEGER'),
            bigquery.SchemaField('score_fiabilite',     'FLOAT'),
            bigquery.SchemaField('diagnostic',          'STRING'),
            bigquery.SchemaField('trimestre',           'DATE'),
            bigquery.SchemaField('label_trimestre',     'STRING'),
            bigquery.SchemaField('position_dans_cycle', 'INTEGER'),
            bigquery.SchemaField('label_calendaire',    'STRING'),
            bigquery.SchemaField('amplitude_pct',       'FLOAT'),
            bigquery.SchemaField('direction',           'STRING'),
            bigquery.SchemaField('marqueur',            'STRING'),
        ]
    )

    job = client.load_table_from_dataframe(df_upload, OUTPUT_TABLE, job_config=job_config)
    job.result()
    print(f"Table uploadee : {OUTPUT_TABLE}")

    print("""
======================================================================
CONFIGURATION LOOKER STUDIO
======================================================================
Source : BigQuery -> m2bpo_data.cycles_profils_v5

Graphique courbe :
  Dimension (axe X) : trimestre        [date, tri chronologique]
  Metrique  (axe Y) : amplitude_pct    [% ecart a la moyenne]
  Repartition       : categorie        [une courbe par segment]

Filtres :
  dimension    -> ex: "MO (type)" pour comparer tous les MO
  diagnostic   -> "Fiable" uniquement
  categorie    -> selection multi pour comparer

Resultat :
  Commune  (cycle 1 an)  -> courbe qui monte/descend 10 fois sur 40 trim.
  Region   (cycle 2 ans) -> courbe qui monte/descend 5 fois sur 40 trim.
  Musee    (cycle 3 ans) -> courbe qui monte/descend 3 fois sur 40 trim.
  -> tous sur le meme axe X (vrais trimestres 2015-Q1 a 2024-Q4)

Colonnes utiles supplementaires :
  label_calendaire  -> infobulle : "Q2 (avr-juin)" ou "An2-Q4 (oct-dec)"
  marqueur          -> pour colorier PIC et CREUX
  position_dans_cycle -> pour filtrer ou trier dans le cycle
======================================================================
""")
