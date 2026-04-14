"""
=============================================================================
M2BPO — ANALYSE IMPACT ÉLECTIONS SUR MARCHÉS PUBLICS
Indices composites base 100 — Dynamique électorale
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import TwoSlopeNorm
import os
import warnings
warnings.filterwarnings("ignore")

# =============================================================================
# 0. CONFIGURATION — SEUL ENDROIT À MODIFIER
# =============================================================================

DATA_PATH   = "data_electionsV3.csv"          # ← ton fichier
OUTPUT_DIR  = "resultats_final"

# Colonnes
COL_ID        = "ID"
COL_DATE      = "inseree_le"
COL_MO        = "MO_type_"
COL_MARCHE    = "marche_cat"
COL_PROCEDURE = "procedure_annonce"

# Colonnes phases déjà calculées
PHASE_COLS = {
    "Munici_2020" : "phase_muni_2020",
    "Pres_2017"   : "phase_pres_2017",
    "Pres_2022"   : "phase_pres_2022",
    "Munici_2026" : "phase_muni_2026",
}
ELECTION_TYPE = {
    "Munici_2020" : "municipale",
    "Munici_2026" : "municipale",
    "Pres_2017"   : "presidentielle",
    "Pres_2022"   : "presidentielle",
}

# ── Phases disponibles par élection ──────────────────────────────────────────
# Pres_2017  : données à partir du 01/01/2016 → S-4 et S-3 absents
#              (S-4 = ~sept 2015, S-3 = ~déc 2015, hors dataset)
# Munici_2026: données jusqu'au 31/12/2025  → S0 et S+x absents (futur)
PHASES_DISPONIBLES = {
    "Munici_2020" : ["S-4","S-3","S-2","S-1","S0","S+1","S+2","S+3","S+4"],
    "Pres_2017"   : ["S-2","S-1","S0","S+1","S+2","S+3","S+4"],   # S-4/S-3 absents
    "Pres_2022"   : ["S-4","S-3","S-2","S-1","S0","S+1","S+2","S+3","S+4"],
    "Munici_2026" : ["S-4","S-3","S-2","S-1"],                     # S0+ absents (futur)
}

# Base 100 = première phase disponible de chaque élection
BASE_PHASE_PAR_ELECTION = {
    "Munici_2020" : "S-4",
    "Pres_2017"   : "S-2",   # ← base S-2 car S-4/S-3 absents
    "Pres_2022"   : "S-4",
    "Munici_2026" : "S-4",
}

PHASES_ORDER = ["S-4","S-3","S-2","S-1","S0","S+1","S+2","S+3","S+4"]

# ── FILTRES VOLUME ────────────────────────────────────────────────────────────
MIN_ANNONCES_PROCEDURE = 12_000   # procédures avec moins → exclues
MIN_ANNONCES_MO        =  3_000   # MO_type_ avec moins  → exclus
TOP_N_MARCHE           =      3   # garder uniquement les 3 marche_cat les + gros

# ── SEUILS QUALITÉ ANALYSE ────────────────────────────────────────────────────
MIN_OBS_PAR_PHASE            = 10   # observations minimum par cellule phase×combo
SEUIL_AMPLITUDE_PARLANT      = 20   # amplitude < 20 → peu parlant
SEUIL_SENSITIVITY_PARLANT    = 10   # sensitivity < 10 → peu parlant

# =============================================================================
# 1. CHARGEMENT + FILTRES
# =============================================================================

def load_and_filter(path: str) -> pd.DataFrame:

    print("=" * 65)
    print("  CHARGEMENT & FILTRES")
    print("=" * 65)

    df = pd.read_csv(path, low_memory=False) if not path.endswith(".parquet") \
         else pd.read_parquet(path)

    df[COL_DATE] = pd.to_datetime(df[COL_DATE], dayfirst=True, errors="coerce")
    for col in [COL_MO, COL_MARCHE, COL_PROCEDURE]:
        df[col] = df[col].astype(str).str.strip()

    n0 = len(df)
    print(f"\n  Lignes brutes : {n0:,}")
    print(f"  Période       : {df[COL_DATE].min().date()} → {df[COL_DATE].max().date()}")

    # ── FILTRE 1 : procedure_annonce ─────────────────────────────────────────
    print(f"\n  {'─'*60}")
    print(f"  FILTRE 1 — procedure_annonce >= {MIN_ANNONCES_PROCEDURE:,} annonces")
    print(f"  {'─'*60}")
    proc_vol = df[COL_PROCEDURE].value_counts()
    for p, n in proc_vol.items():
        tag = "✅ gardée" if n >= MIN_ANNONCES_PROCEDURE else "❌ exclue"
        print(f"    {tag}  {p:<45} {n:>9,}")
    procs_ok = proc_vol[proc_vol >= MIN_ANNONCES_PROCEDURE].index.tolist()
    df = df[df[COL_PROCEDURE].isin(procs_ok)].copy()
    print(f"\n  → {len(procs_ok)} procédures gardées | {len(df):,} lignes restantes")

    # ── FILTRE 2 : MO_type_ ──────────────────────────────────────────────────
    print(f"\n  {'─'*60}")
    print(f"  FILTRE 2 — MO_type_ >= {MIN_ANNONCES_MO:,} annonces")
    print(f"  {'─'*60}")
    mo_vol = df[COL_MO].value_counts()
    for m, n in mo_vol.items():
        tag = "✅ gardé " if n >= MIN_ANNONCES_MO else "❌ exclu "
        print(f"    {tag}  {m:<50} {n:>9,}")
    mo_ok = mo_vol[mo_vol >= MIN_ANNONCES_MO].index.tolist()
    df = df[df[COL_MO].isin(mo_ok)].copy()
    print(f"\n  → {len(mo_ok)} MO_type_ gardés | {len(df):,} lignes restantes")

    # ── FILTRE 3 : marche_cat top N ──────────────────────────────────────────
    print(f"\n  {'─'*60}")
    print(f"  FILTRE 3 — marche_cat : top {TOP_N_MARCHE} par volume")
    print(f"  {'─'*60}")
    marche_vol = df[COL_MARCHE].value_counts()
    for i, (m, n) in enumerate(marche_vol.items()):
        tag = "✅ gardée" if i < TOP_N_MARCHE else "❌ exclue"
        print(f"    {tag}  {m:<45} {n:>9,}")
    marche_ok = marche_vol.head(TOP_N_MARCHE).index.tolist()
    df = df[df[COL_MARCHE].isin(marche_ok)].copy()
    print(f"\n  → {len(marche_ok)} marche_cat gardés : {marche_ok}")

    # ── BILAN FINAL ──────────────────────────────────────────────────────────
    print(f"\n  {'═'*60}")
    print(f"  BILAN — {n0:,} → {len(df):,} lignes ({len(df)/n0*100:.1f}% du dataset)")
    print(f"  MO_type_    : {len(mo_ok)}")
    print(f"  marche_cat  : {len(marche_ok)}")
    print(f"  procedures  : {len(procs_ok)}")
    print(f"  {'═'*60}")

    return df


# =============================================================================
# 2. CALCUL INDICE BASE 100
# =============================================================================
# PRINCIPE :
#   Pour chaque combo (ex: Ville × Bâtiment × Procédure Ouverte) :
#     → on compte les annonces par phase disponible
#     → on divise CHAQUE phase par le count de la phase BASE du même combo × 100
#
#   Base par élection :
#     Munici_2020 / Pres_2022 → base S-4
#     Pres_2017               → base S-2  (S-4/S-3 hors dataset)
#     Munici_2026             → base S-4  (S0+ futur non disponible)
#
#   Un combo avec 50 annonces et un avec 50 000 sont comparables.
#   On mesure la VARIATION RELATIVE = la dynamique électorale.
# =============================================================================

def calc_indice_base100(df: pd.DataFrame,
                         election: str,
                         group_cols: list) -> pd.DataFrame:
    """
    Calcule l'indice base 100 pour chaque (combo × élection).

    Base et phases disponibles par élection :
      Munici_2020 → base S-4 | phases S-4..S+4  (cycle complet)
      Pres_2017   → base S-2 | phases S-2..S+4  (S-4/S-3 hors dataset)
      Pres_2022   → base S-4 | phases S-4..S+4  (cycle complet)
      Munici_2026 → base S-4 | phases S-4..S-1  (S0+ futur non disponible)

    Normalisation : idx_phase = (n_annonces_phase / n_annonces_BASE) × 100
    Chaque combo est indexé sur lui-même → volumes non comparés entre combos.
    """
    phase_col    = PHASE_COLS[election]
    base_phase   = BASE_PHASE_PAR_ELECTION[election]
    phases_dispo = PHASES_DISPONIBLES[election]

    # Garder uniquement les phases disponibles pour cette élection
    sub = df[df[phase_col].isin(phases_dispo)].copy()
    sub["phase"] = sub[phase_col]

    if sub.empty:
        return pd.DataFrame()

    # Comptage annonces par combo × phase
    counts = (
        sub.groupby(group_cols + ["phase"])[COL_ID]
        .count()
        .reset_index(name="n_obs")
    )
    counts = counts[counts["n_obs"] >= MIN_OBS_PAR_PHASE]

    # Pivot : une colonne par phase disponible
    pivot = counts.pivot_table(
        index=group_cols, columns="phase",
        values="n_obs", fill_value=np.nan
    ).reset_index()

    # Vérifier que la phase de base existe dans les données
    if base_phase not in pivot.columns:
        print(f"    ⚠️  {election} : base '{base_phase}' absente → ignoré")
        return pd.DataFrame()

    pivot = pivot[pivot[base_phase] > 0].copy()
    if pivot.empty:
        return pd.DataFrame()

    # ── NORMALISATION BASE 100 ───────────────────────────────────────────────
    # Division par le count de la phase de base DU MÊME combo
    # Un combo à 200 annonces et un à 20 000 sont parfaitement comparables
    base_vals = pivot[base_phase]
    for phase in PHASES_ORDER:
        if phase in pivot.columns:
            pivot[f"idx_{phase}"] = (pivot[phase] / base_vals * 100).round(1)
        else:
            pivot[f"idx_{phase}"] = np.nan   # phase non dispo → NaN explicite

    # Garder group_cols + idx_*
    idx_cols  = [f"idx_{p}" for p in PHASES_ORDER]
    pivot = pivot[group_cols + [c for c in idx_cols if c in pivot.columns]].copy()

    pivot["election"]      = election
    pivot["election_type"] = ELECTION_TYPE[election]
    pivot["base_phase"]    = base_phase   # traçabilité
    return pivot


def calc_tous_indices(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """Lance le calcul pour les 4 élections et concatène."""
    frames = []
    for elec in PHASE_COLS:
        res = calc_indice_base100(df, elec, group_cols)
        if not res.empty:
            frames.append(res)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =============================================================================
# 3. SCORES DE DYNAMIQUE ÉLECTORALE
# =============================================================================

def calc_scores(indices: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    Pour chaque combo × élection, calcule :

    drop_score    : idx_S0 - idx_S-1
                    → négatif = gel électoral (activité freine avant/pendant)
                    → positif = pas de gel

    rebound_score : max(idx_S+1 .. idx_S+4) - idx_S0
                    → positif = rebond post-électoral
                    → proche 0 = pas de rebond

    sensitivity   : rebound_score - drop_score
                    → score synthétique de réactivité électorale
                    → PLUS C'EST ÉLEVÉ, PLUS LE COMBO EST DYNAMIQUE

    amplitude     : max(tous idx) - min(tous idx)
                    → variation totale sur le cycle complet

    mean_post     : moyenne idx_S+1 .. idx_S+4
                    → niveau d'activité post-élection vs baseline
    """
    idx_all  = [f"idx_{p}" for p in PHASES_ORDER if f"idx_{p}" in indices.columns]
    post     = [c for c in ["idx_S+1","idx_S+2","idx_S+3","idx_S+4"] if c in idx_all]

    out = indices[group_cols + ["election","election_type"]].copy()

    # Drop (gel pré/péri-électoral)
    s_minus1 = indices.get("idx_S-1", pd.Series(np.nan, index=indices.index))
    s0       = indices.get("idx_S0",  pd.Series(np.nan, index=indices.index))
    out["drop_score"]    = (s0 - s_minus1).round(1)

    # Rebound (rattrapage post-électoral)
    if post:
        out["rebound_score"] = (indices[post].max(axis=1) - s0).round(1)
    else:
        out["rebound_score"] = np.nan

    # Sensitivity = signal net de dynamique électorale
    out["sensitivity"]   = (out["rebound_score"].fillna(0)
                            - out["drop_score"].fillna(0)).round(1)

    # Amplitude totale
    out["amplitude"]     = (indices[idx_all].max(axis=1)
                           - indices[idx_all].min(axis=1)).round(1)

    # Niveau moyen post-électoral
    if post:
        out["mean_post"] = indices[post].mean(axis=1).round(1)

    return out


def agregation_scores(scores: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    Moyenne des scores sur les 4 élections pour chaque combo.
    Ajoute aussi les scores séparés munici / présidentielle.
    """
    # Moyenne globale
    agg = (
        scores.groupby(group_cols)
        .agg(
            drop_moy        = ("drop_score",    "mean"),
            rebound_moy     = ("rebound_score", "mean"),
            sensitivity_moy = ("sensitivity",   "mean"),
            amplitude_moy   = ("amplitude",     "mean"),
            mean_post_moy   = ("mean_post",     "mean"),
            n_elections     = ("election",      "count"),
        )
        .reset_index()
    )

    # Sensitivity séparée par type d'élection
    for etype, elabel in [("municipale","muni"), ("presidentielle","pres")]:
        sub = (scores[scores["election_type"] == etype]
               .groupby(group_cols)["sensitivity"]
               .mean()
               .rename(f"sensitivity_{elabel}"))
        agg = agg.merge(sub, on=group_cols, how="left")

    # Statut parlant / peu parlant
    agg["statut"] = np.where(
        (agg["amplitude_moy"]   >= SEUIL_AMPLITUDE_PARLANT) &
        (agg["sensitivity_moy"] >= SEUIL_SENSITIVITY_PARLANT),
        "✅ Parlant", "⚠️  Peu parlant"
    )

    # Quadrant dynamique
    def quadrant(row):
        m = row.get("sensitivity_muni", 0) or 0
        p = row.get("sensitivity_pres", 0) or 0
        if m > 10 and p > 10: return "🟢 Bi-sensible"
        if m > 10:             return "🟠 Sensible Munici"
        if p > 10:             return "🔵 Sensible Présid"
        return "🔴 Peu parlant"

    agg["quadrant"] = agg.apply(quadrant, axis=1)

    return agg.sort_values("sensitivity_moy", ascending=False)


# =============================================================================
# 4. AFFICHAGE CONSOLE — CLASSEMENT
# =============================================================================

def afficher_classement(agg: pd.DataFrame, group_cols: list, label: str):
    cols_affich = group_cols + ["sensitivity_moy","amplitude_moy",
                                "drop_moy","rebound_moy","quadrant","statut"]
    cols_ok = [c for c in cols_affich if c in agg.columns]

    print(f"\n{'═'*80}")
    print(f"  CLASSEMENT DYNAMIQUE — {label.upper()}")
    print(f"{'═'*80}")

    parlants  = agg[agg["statut"] == "✅ Parlant"]
    peu       = agg[agg["statut"] == "⚠️  Peu parlant"]

    print(f"\n  ✅ PARLANTS ({len(parlants)}) — triés par sensitivity décroissante :")
    print(f"  {'Combo':<55} {'Sens':>6}  {'Amp':>6}  {'Drop':>6}  {'Rebond':>7}  {'Quadrant'}")
    print(f"  {'─'*54} {'─'*6}  {'─'*6}  {'─'*6}  {'─'*7}  {'─'*20}")
    for _, r in parlants.iterrows():
        combo = " × ".join([str(r[c]) for c in group_cols])[:54]
        print(f"  {combo:<55} "
              f"{r['sensitivity_moy']:>6.0f}  "
              f"{r['amplitude_moy']:>6.0f}  "
              f"{r['drop_moy']:>6.0f}  "
              f"{r['rebound_moy']:>7.0f}  "
              f"{r.get('quadrant','')}")

    print(f"\n  ⚠️  PEU PARLANTS à écarter ({len(peu)}) :")
    for _, r in peu.iterrows():
        combo = " × ".join([str(r[c]) for c in group_cols])
        print(f"    → {combo}")


# =============================================================================
# 5. VISUALISATIONS
# =============================================================================

def combo_label(row, group_cols):
    return " × ".join([str(row[c]) for c in group_cols])


# ── 5a. Heatmap sensibilité combo × élection ─────────────────────────────────
def plot_heatmap(scores: pd.DataFrame, group_cols: list,
                 label: str, save: str = None):

    df = scores.copy()
    df["combo"] = df.apply(lambda r: combo_label(r, group_cols), axis=1)

    pivot = df.pivot_table(index="combo", columns="election",
                           values="sensitivity", aggfunc="mean")
    pivot = pivot.loc[pivot.mean(axis=1).sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(10, max(5, len(pivot) * 0.4)))

    # TwoSlopeNorm exige strictement vmin < vcenter < vmax
    # On calcule une norm robuste qui fonctionne dans tous les cas
    vmin = float(np.nanmin(pivot.values))
    vmax = float(np.nanmax(pivot.values))

    if vmin < 0 < vmax:
        # Cas normal : valeurs négatives et positives → centrer sur 0
        norm = TwoSlopeNorm(vmin=vmin - 0.01, vcenter=0, vmax=vmax + 0.01)
    else:
        # Toutes les valeurs du même signe → pas de TwoSlopeNorm, on use vmin/vmax
        norm = plt.Normalize(vmin=vmin - 0.01, vmax=vmax + 0.01)

    sns.heatmap(pivot, annot=True, fmt=".0f", cmap="RdYlGn",
                norm=norm, linewidths=0.5, ax=ax, annot_kws={"size": 9})
    ax.set_title(f"Dynamique électorale (base 100 en S-4)\n{label}",
                 fontsize=12, fontweight="bold")
    ax.set_xlabel("Élection")
    ax.set_ylabel("Combinaison")
    ax.tick_params(axis="y", labelsize=7)
    plt.tight_layout()
    if save:
        plt.savefig(save, dpi=150, bbox_inches="tight")
        print(f"  💾 {save}")
    plt.show()


# ── 5b. Courbes indice base 100 — top N combos ───────────────────────────────
def plot_courbes(indices: pd.DataFrame, scores: pd.DataFrame,
                 group_cols: list, label: str,
                 top_n: int = 9, save: str = None):

    top = (scores.groupby(group_cols)["sensitivity"]
           .mean().reset_index()
           .sort_values("sensitivity", ascending=False)
           .head(top_n))

    phases_dispo = [p for p in PHASES_ORDER if f"idx_{p}" in indices.columns]
    couleurs = {
        "Munici_2020": "#FF8C00",
        "Munici_2026": "#FF4500",
        "Pres_2017"  : "#1E90FF",
        "Pres_2022"  : "#00008B",
    }

    n_cols = 3
    n_rows = (top_n + n_cols - 1) // n_cols
    fig, axes = plt.subplots(n_rows, n_cols,
                              figsize=(18, n_rows * 4.2))
    axes = axes.flatten()

    for i, (_, row_combo) in enumerate(top.iterrows()):
        ax = axes[i]

        # Filtrer les lignes de ce combo
        mask = pd.Series([True] * len(indices))
        for col in group_cols:
            mask = mask & (indices[col] == row_combo[col])
        data_combo = indices[mask]

        for _, row_elec in data_combo.iterrows():
            y = [row_elec.get(f"idx_{p}", np.nan) for p in phases_dispo]
            elec = row_elec["election"]
            ax.plot(phases_dispo, y,
                    marker="o", markersize=5, linewidth=2,
                    color=couleurs.get(elec, "gray"),
                    label=elec, alpha=0.9)

        # Ligne base 100
        ax.axhline(100, color="gray", linestyle="--",
                   linewidth=0.8, alpha=0.5, label="_base 100")
        # Ligne S0 (jour élection)
        if "S0" in phases_dispo:
            ax.axvline("S0", color="red", linestyle=":",
                       linewidth=1.2, alpha=0.6)
            ax.text("S0", ax.get_ylim()[0], " J élection",
                    color="red", fontsize=6, va="bottom")

        titre = combo_label(row_combo, group_cols)
        ax.set_title(titre, fontsize=8, fontweight="bold")
        ax.set_xlabel("Phase (4 semaines)", fontsize=7)
        ax.set_ylabel("Indice (S-4 = 100)", fontsize=7)
        ax.tick_params(axis="x", labelsize=7, rotation=45)
        ax.legend(fontsize=6, loc="upper right")
        ax.grid(True, alpha=0.2)

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(
        f"Top {top_n} combos — dynamique base 100\n{label}\n"
        f"(chaque courbe = variation RELATIVE par rapport à S-4, indépendamment du volume)",
        fontsize=11, fontweight="bold"
    )
    plt.tight_layout()
    if save:
        plt.savefig(save, dpi=150, bbox_inches="tight")
        print(f"  💾 {save}")
    plt.show()


# ── 5c. Scatter Munici vs Présidentielle ─────────────────────────────────────
def plot_scatter(scores: pd.DataFrame, group_cols: list,
                 label: str, save: str = None):

    df = scores.copy()
    df["combo"] = df.apply(lambda r: combo_label(r, group_cols), axis=1)

    muni = (df[df["election_type"] == "municipale"]
            .groupby("combo")["sensitivity"].mean().rename("muni"))
    pres = (df[df["election_type"] == "presidentielle"]
            .groupby("combo")["sensitivity"].mean().rename("pres"))
    sc = pd.concat([muni, pres], axis=1).dropna()

    def color_point(row):
        if row["muni"] > 10 and row["pres"] > 10: return "#2ecc71"
        if row["muni"] > 10:                       return "#f39c12"
        if row["pres"] > 10:                       return "#3498db"
        return "#e74c3c"

    fig, ax = plt.subplots(figsize=(13, 9))
    colors = [color_point(r) for _, r in sc.iterrows()]
    ax.scatter(sc["muni"], sc["pres"], c=colors, s=90,
               alpha=0.85, edgecolors="white", linewidth=0.5)

    for name, row in sc.iterrows():
        ax.annotate(name, (row["muni"], row["pres"]),
                    fontsize=6.5, ha="center", va="bottom",
                    xytext=(0, 4), textcoords="offset points")

    ax.axhline(0,  color="#bdc3c7", linestyle="--", lw=0.9)
    ax.axvline(0,  color="#bdc3c7", linestyle="--", lw=0.9)
    ax.axhline(10, color="#bdc3c7", linestyle=":",  lw=0.7)
    ax.axvline(10, color="#bdc3c7", linestyle=":",  lw=0.7)

    xl, yl = ax.get_xlim(), ax.get_ylim()
    ax.text(xl[1]*0.65, yl[1]*0.92, "🟢 Bi-sensible",      color="#27ae60", fontsize=9, fontweight="bold")
    ax.text(xl[0]*0.65, yl[1]*0.92, "🔵 Sensible Présid.",  color="#2980b9", fontsize=9, fontweight="bold")
    ax.text(xl[1]*0.65, yl[0]*0.92, "🟠 Sensible Munici.",  color="#d35400", fontsize=9, fontweight="bold")
    ax.text(xl[0]*0.65, yl[0]*0.92, "🔴 Peu parlant",       color="#c0392b", fontsize=9, fontweight="bold")

    ax.set_xlabel("Sensitivity Municipales (moy. 2020 & 2026)", fontsize=10)
    ax.set_ylabel("Sensitivity Présidentielles (moy. 2017 & 2022)", fontsize=10)
    ax.set_title(f"Carte dynamique électorale — {label}\n(scores base 100, indépendants du volume)",
                 fontsize=11, fontweight="bold")
    ax.grid(True, alpha=0.2)
    plt.tight_layout()
    if save:
        plt.savefig(save, dpi=150, bbox_inches="tight")
        print(f"  💾 {save}")
    plt.show()


# ── 5d. Barres Gel vs Rebond ──────────────────────────────────────────────────
def plot_bars_gel_rebond(scores: pd.DataFrame, group_cols: list,
                          label: str, top_n: int = 15, save: str = None):

    agg = (
        scores.groupby(group_cols)
        .agg(drop    = ("drop_score",    "mean"),
             rebound = ("rebound_score", "mean"),
             amp     = ("amplitude",     "mean"))
        .reset_index()
        .sort_values("amp", ascending=False)
        .head(top_n)
    )
    agg["combo"] = agg.apply(lambda r: combo_label(r, group_cols), axis=1)
    agg = agg.sort_values("rebound", ascending=True)

    fig, ax = plt.subplots(figsize=(11, max(6, top_n * 0.45)))
    y = np.arange(len(agg))

    ax.barh(y + 0.2, agg["rebound"], height=0.38,
            color="#2ecc71", alpha=0.85, label="Rebond post-électoral")
    ax.barh(y - 0.2, agg["drop"],    height=0.38,
            color="#e74c3c", alpha=0.85, label="Gel pré-électoral")

    ax.set_yticks(y)
    ax.set_yticklabels(agg["combo"], fontsize=8)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Variation indice base 100 (points)", fontsize=9)
    ax.set_title(f"Gel pré-électoral vs Rebond post-électoral\n{label}",
                 fontsize=11, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, axis="x", alpha=0.25)
    plt.tight_layout()
    if save:
        plt.savefig(save, dpi=150, bbox_inches="tight")
        print(f"  💾 {save}")
    plt.show()


# =============================================================================
# 6. EXPORT EXCEL
# =============================================================================

def export_excel(all_data: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "indices_composites_final.xlsx")

    summary = []
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, data in all_data.items():
            safe = name[:20]
            data["ranking"].to_excel(writer, sheet_name=f"{safe}_ranking", index=False)
            data["indices"].to_excel(writer, sheet_name=f"{safe}_indices", index=False)
            data["scores"].to_excel(writer,  sheet_name=f"{safe}_scores",  index=False)
            top = data["ranking"].head(5).copy()
            top["analyse"] = name
            summary.append(top)

        # Onglet résumé global
        pd.concat(summary, ignore_index=True).to_excel(
            writer, sheet_name="RESUME_TOP5", index=False)

        # Onglet peu parlants
        peu_list = []
        for name, data in all_data.items():
            pp = data["ranking"][data["ranking"]["statut"] == "⚠️  Peu parlant"].copy()
            pp["analyse"] = name
            peu_list.append(pp)
        pd.concat(peu_list, ignore_index=True).to_excel(
            writer, sheet_name="PEU_PARLANTS", index=False)

    print(f"\n  ✅ Excel : {path}")


# =============================================================================
# 7. PIPELINE PRINCIPALE
# =============================================================================

ANALYSES = {
    "MO_type"            : [COL_MO],
    "marche_cat"         : [COL_MARCHE],
    "procedure"          : [COL_PROCEDURE],
    "MO x marche"        : [COL_MO, COL_MARCHE],
    "MO x procedure"     : [COL_MO, COL_PROCEDURE],
    "marche x procedure" : [COL_MARCHE, COL_PROCEDURE],
    "Triple combo"       : [COL_MO, COL_MARCHE, COL_PROCEDURE],
}

if __name__ == "__main__":

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── 1. Chargement + filtres ──────────────────────────────────────────────
    df = load_and_filter(DATA_PATH)

    all_data = {}

    # ── 2. Calcul pour chaque niveau d'analyse ───────────────────────────────
    print(f"\n{'='*65}")
    print("  CALCUL DES INDICES BASE 100 PAR ANALYSE")
    print(f"{'='*65}")

    for name, gcols in ANALYSES.items():
        print(f"\n▶  {name} ...", end=" ")

        indices = calc_tous_indices(df, gcols)
        if indices.empty:
            print("⚠️  données insuffisantes")
            continue

        scores  = calc_scores(indices, gcols)
        ranking = agregation_scores(scores, gcols)

        n_ok  = (ranking["statut"] == "✅ Parlant").sum()
        n_peu = (ranking["statut"] == "⚠️  Peu parlant").sum()
        print(f"✅ {n_ok} parlants  ⚠️  {n_peu} peu parlants")

        all_data[name] = {"indices": indices, "scores": scores, "ranking": ranking}

        # Affichage classement console
        afficher_classement(ranking, gcols, name)

    # ── 3. Visualisations ────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print("  GÉNÉRATION DES GRAPHIQUES")
    print(f"{'='*65}")

    for name, gcols in ANALYSES.items():
        if name not in all_data:
            continue
        safe = name.replace(" ", "_").replace("×","x")
        print(f"\n📊 {name}")

        plot_heatmap(
            all_data[name]["scores"], gcols, label=name,
            save=f"{OUTPUT_DIR}/heatmap_{safe}.png")

        plot_courbes(
            all_data[name]["indices"], all_data[name]["scores"],
            gcols, label=name, top_n=9,
            save=f"{OUTPUT_DIR}/courbes_{safe}.png")

        plot_scatter(
            all_data[name]["scores"], gcols, label=name,
            save=f"{OUTPUT_DIR}/scatter_{safe}.png")

        plot_bars_gel_rebond(
            all_data[name]["scores"], gcols, label=name, top_n=15,
            save=f"{OUTPUT_DIR}/bars_{safe}.png")

    # ── 4. Export Excel ──────────────────────────────────────────────────────
    export_excel(all_data, OUTPUT_DIR)

    # ── 5. Résumé final console ───────────────────────────────────────────────
    print(f"\n{'='*65}")
    print("  RÉSUMÉ FINAL — COMBOS LES PLUS DYNAMIQUES")
    print(f"{'='*65}")

    for name in ["MO x marche", "MO x procedure", "Triple combo"]:
        if name not in all_data:
            continue
        gcols   = ANALYSES[name]
        ranking = all_data[name]["ranking"]
        parlant = ranking[ranking["statut"] == "✅ Parlant"]
        peu     = ranking[ranking["statut"] == "⚠️  Peu parlant"]

        print(f"\n  ── {name} ──")
        print(f"  ✅ Parlants ({len(parlant)}) :")
        for _, r in parlant.iterrows():
            c = " × ".join([str(r[col]) for col in gcols])
            print(f"    {c:<65} sens={r['sensitivity_moy']:.0f}  "
                  f"amp={r['amplitude_moy']:.0f}  {r.get('quadrant','')}")
        print(f"  ⚠️  Peu parlants à écarter : {len(peu)}")

    print(f"\n  Résultats sauvegardés dans : {OUTPUT_DIR}/")
    print(f"  Fichier principal          : {OUTPUT_DIR}/indices_composites_final.xlsx")
