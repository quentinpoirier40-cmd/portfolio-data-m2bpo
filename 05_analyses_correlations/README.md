# 05_analyses_correlations

Corrélations entre volume d'annonces et facteurs exogènes : indices BT01/PT01 et cycles électoraux.

## Fichiers

- `scripts/correlation_indices_BT01_PT01.py` : corrélation croisée (lag 0 à 3) entre variation des indices et volume d'annonces, par segment (MO, procédure, marché).
- `scripts/correlation_elections_base100.py` : analyse de l'impact des élections (présidentielles 2017/2022, municipales 2020/2026) via indice base 100 et scores composites (drop, rebound, sensitivity).
- `scripts/analyse_cycles_elections.py` : combinaison des deux approches (optionnel).

## Résultats clés

- **Gel pré-électoral** systématique (S-1 à S0) suivi d'un **rebond** post-électoral (S+2 à S+4).
- Certains segments (`MO public × procédure restreinte × marché bâtiment`) affichent une **sensitivity > 30**, soit une réactivité très forte aux échéances.
- Corrélation modérée entre BT01 et volume des annonces de bâtiment (r ≈ 0.4 à lag 1 trimestre).

## Utilisation

Lancer les scripts Python après avoir chargé les données nettoyées depuis BigQuery ou depuis les CSV.
