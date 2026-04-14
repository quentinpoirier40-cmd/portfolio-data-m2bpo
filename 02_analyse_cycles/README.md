# 02_analyse_cycles

Détection automatique des cycles saisonniers dans les appels d'offres.

## Méthodologie

1. **FFT (Fast Fourier Transform)** : détection de la période dominante sans a priori.
2. **Seasonal decompose** (statsmodels) : validation et calcul d'un score de fiabilité (0 à 1).
3. **Règle des 3 répétitions** : période max = n_trimestres // 3.
4. **Seuils** : cycles calendaires (6 mois, 1 an, 2 ans, 3 ans) : score ≥ 0.40 ; cycles insolites : score ≥ 0.80.

## Résultats

- **137 segments cycliques** identifiés sur 5 dimensions simples et leurs combinaisons.
- **62 cycles annuels** (période 4 trimestres), dont un score max de **0.854** (Logement/Bâtiment/MOP).
- **Pic en Q1** (janvier-mars) pour 55% des segments.

## Fichiers

- `scripts/detection_cycles_fft_v5.py` : script Python complet (v5 avec projection sur historique).
- `scripts/cycles_profils_v5.sql` : requête BigQuery pour matérialiser la table `cycles_profils_v5`.

## Utilisation dans Looker Studio

La table `cycles_profils_v5` contient une ligne par trimestre réel et par segment, avec :
- `amplitude_pct` : écart à la moyenne (en %) → métrique pour la courbe
- `trimestre` : axe X (date)
- `categorie` : répartition (une courbe par segment)
