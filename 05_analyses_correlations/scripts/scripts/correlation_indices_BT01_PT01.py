from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
from scipy import stats

# ── AUTHENTIFICATION ────────────────────────────────────────
credentials = service_account.Credentials.from_service_account_file(
    '/home/quentin_poirier/scraper_emails/XXXXXX.json'
)

# ── 7 NIVEAUX ───────────────────────────────────────────────
queries = {
    'MO': {
        'sql': """
            SELECT CAST(DATE_TRUNC(inseree_le, QUARTER) AS STRING) AS trimestre,
              MO_type_,
              COUNT(*) AS nb_annonces,
              ANY_VALUE(INDICEBT01) AS BT01, ANY_VALUE(INDICEPT01) AS PT01
            FROM `m2bpo-data-analyse.m2bpo_data.10yrtenders_with_indices`
            GROUP BY trimestre, MO_type_
        """,
        'cols': ['MO_type_']
    },
    'Marche': {
        'sql': """
            SELECT CAST(DATE_TRUNC(inseree_le, QUARTER) AS STRING) AS trimestre,
              marche_cat,
              COUNT(*) AS nb_annonces,
              ANY_VALUE(INDICEBT01) AS BT01, ANY_VALUE(INDICEPT01) AS PT01
            FROM `m2bpo-data-analyse.m2bpo_data.10yrtenders_with_indices`
            GROUP BY trimestre, marche_cat
        """,
        'cols': ['marche_cat']
    },
    'Procedure': {
        'sql': """
            SELECT CAST(DATE_TRUNC(inseree_le, QUARTER) AS STRING) AS trimestre,
              procedure_annonce,
              COUNT(*) AS nb_annonces,
              ANY_VALUE(INDICEBT01) AS BT01, ANY_VALUE(INDICEPT01) AS PT01
            FROM `m2bpo-data-analyse.m2bpo_data.10yrtenders_with_indices`
            GROUP BY trimestre, procedure_annonce
        """,
        'cols': ['procedure_annonce']
    },
    'MO x Marche': {
        'sql': """
            SELECT CAST(DATE_TRUNC(inseree_le, QUARTER) AS STRING) AS trimestre,
              MO_type_, marche_cat,
              COUNT(*) AS nb_annonces,
              ANY_VALUE(INDICEBT01) AS BT01, ANY_VALUE(INDICEPT01) AS PT01
            FROM `m2bpo-data-analyse.m2bpo_data.10yrtenders_with_indices`
            GROUP BY trimestre, MO_type_, marche_cat
        """,
        'cols': ['MO_type_', 'marche_cat']
    },
    'MO x Procedure': {
        'sql': """
            SELECT CAST(DATE_TRUNC(inseree_le, QUARTER) AS STRING) AS trimestre,
              MO_type_, procedure_annonce,
              COUNT(*) AS nb_annonces,
              ANY_VALUE(INDICEBT01) AS BT01, ANY_VALUE(INDICEPT01) AS PT01
            FROM `m2bpo-data-analyse.m2bpo_data.10yrtenders_with_indices`
            GROUP BY trimestre, MO_type_, procedure_annonce
        """,
        'cols': ['MO_type_', 'procedure_annonce']
    },
    'Marche x Procedure': {
        'sql': """
            SELECT CAST(DATE_TRUNC(inseree_le, QUARTER) AS STRING) AS trimestre,
              marche_cat, procedure_annonce,
              COUNT(*) AS nb_annonces,
              ANY_VALUE(INDICEBT01) AS BT01, ANY_VALUE(INDICEPT01) AS PT01
            FROM `m2bpo-data-analyse.m2bpo_data.10yrtenders_with_indices`
            GROUP BY trimestre, marche_cat, procedure_annonce
        """,
        'cols': ['marche_cat', 'procedure_annonce']
    },
    'Triple': {
        'sql': """
            SELECT CAST(DATE_TRUNC(inseree_le, QUARTER) AS STRING) AS trimestre,
              MO_type_, marche_cat, procedure_annonce,
              COUNT(*) AS nb_annonces,
              ANY_VALUE(INDICEBT01) AS BT01, ANY_VALUE(INDICEPT01) AS PT01
            FROM `m2bpo-data-analyse.m2bpo_data.10yrtenders_with_indices`
            GROUP BY trimestre, MO_type_, marche_cat, procedure_annonce
        """,
        'cols': ['MO_type_', 'marche_cat', 'procedure_annonce']
    },
}

MIN_ANNONCES = 1000

# ── FONCTION CORRÉLATION ────────────────────────────────────
def calcul_correlations(df, cols_segment):
    totaux = df.groupby(cols_segment)['nb_annonces'].sum()
    segments_valides = totaux[totaux >= MIN_ANNONCES].index

    results = []
    for seg in segments_valides:
        if len(cols_segment) == 1:
            mask_seg = df[cols_segment[0]] == seg
        else:
            mask_seg = pd.Series([True] * len(df), index=df.index)
            for col, val in zip(cols_segment, seg):
                mask_seg &= (df[col] == val)

        sub = df[mask_seg].copy().sort_values('trimestre').reset_index(drop=True)

        if len(sub) < 8:
            continue

        sub['BT01_var1t'] = sub['BT01'].diff()
        sub['PT01_var1t'] = sub['PT01'].diff()

        row = {'nb_annonces_total': int(totaux[seg]), 'n_trimestres': len(sub)}

        if len(cols_segment) == 1:
            row[cols_segment[0]] = seg
        else:
            for col, val in zip(cols_segment, seg):
                row[col] = val

        for indice in ['BT01', 'PT01']:
            var_col = f'{indice}_var1t'
            for lag in [0, 1, 2, 3]:
                serie_lag = sub['nb_annonces'].shift(-lag)
                mask = sub[var_col].notna() & serie_lag.notna()
                if mask.sum() >= 6:
                    r, p = stats.pearsonr(sub.loc[mask, var_col], serie_lag[mask])
                    row[f'r_{indice}_T{lag}'] = round(r, 3)
                    row[f'p_{indice}_T{lag}'] = round(p, 3)
                    row[f'sig_{indice}_T{lag}'] = '✅' if p < 0.05 else '❌'
                else:
                    row[f'r_{indice}_T{lag}'] = None
                    row[f'p_{indice}_T{lag}'] = None
                    row[f'sig_{indice}_T{lag}'] = '—'

        r_cols = [f'r_BT01_T{i}' for i in range(4)] + [f'r_PT01_T{i}' for i in range(4)]
        r_vals = [(c, abs(row[c])) for c in r_cols if row.get(c) is not None]

        if r_vals:
            best_col, best_val = max(r_vals, key=lambda x: x[1])
            row['best_r_abs']  = round(best_val, 3)
            row['best_config'] = best_col
            row['direction']   = '↑ indice → ↑ annonces' if row[best_col] > 0 else '↑ indice → ↓ annonces'
        else:
            row['best_r_abs']  = None
            row['best_config'] = '—'
            row['direction']   = '—'

        results.append(row)

    if not results:
        return pd.DataFrame()

    res = pd.DataFrame(results).sort_values('best_r_abs', ascending=False).reset_index(drop=True)
    return res

# ── LANCEMENT ───────────────────────────────────────────────
all_results = []

for niveau, params in queries.items():
    print(f"\nChargement {niveau}...")
    df = client.query(params['sql']).to_dataframe()
    df['trimestre'] = pd.to_datetime(df['trimestre'])
    df = df.sort_values(params['cols'] + ['trimestre']).reset_index(drop=True)
    print(f"  {len(df):,} lignes chargées")

    res = calcul_correlations(df, params['cols'])

    if res.empty:
        print(f"  ⚠️ Aucun résultat pour {niveau}")
        continue

    res['niveau'] = niveau
    all_results.append(res)
    print(f"  {len(res)} segments analysés")

# ── FUSION EN 1 SEUL CSV ────────────────────────────────────
final = pd.concat(all_results, ignore_index=True)
final = final.sort_values(['niveau', 'best_r_abs'], ascending=[True, False]).reset_index(drop=True)
final.insert(0, 'rang', final.index + 1)

print(f"\nTotal lignes : {len(final)}")
print(final.groupby('niveau')['best_r_abs'].count().rename('nb_segments').to_string())

final.to_csv('correlation_indices_tous_niveaux.csv', index=False)
print("\nExporté : correlation_indices_tous_niveaux.csv")
