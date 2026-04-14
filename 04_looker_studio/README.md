# 04_looker_studio

Dashboard interactif développé pour M2BPO.

## Pages

1. **Executive Summary** : KPIs, évolution trimestrielle, top MO.
2. **Analyse Géographique** : carte choroplèthe (montant), bulles par département, top 20 villes.
3. **Structure des Marchés** : treemap, heatmap procédure × contrat, stack MO × contrat.
4. **Focus Concours Restreints** : volume/montant, top départements, analyse croisée.
5. **Compétences & Documents** : top 10 des compétences et documents demandés.
6. **Délais de réponse** : histogramme, scatter urgence (montant vs délai).
7. **Concentration du marché** : courbe de Pareto (cumul volume par MO).
8. **Cycle Municipal** : comparaison 2019-2020 vs 2024-2025, anticipation 2026.
9. **MO Publics vs Privés** : bullet chart, heatmap jour de publication.
10. **Qualité des données** : taux de complétude, anomalies.

## Captures d'écran

Voir dossier `dashboard_screenshots/`.

## Configuration Looker Studio

- Source : BigQuery (`m2bpo-data-analyse.m2bpo_data`)
- Filtres rapport : Année, Région, Catégorie de marché, Procédure
- Chartes : palette M2BPO, coins arrondis, typographie uniforme
