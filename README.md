# Portfolio Data — Mission M2BPO

## Contexte
Mission de data analyst chez M2BPO,logiciel spécialisé
dans les marchés publics pour architect. Analyse de 10 ans d'annonces de marchés publics
(2015-2024) pour identifier des cycles saisonniers exploitables
commercialement.

## Stack technique
- **BigQuery** — stockage et transformation des données
- **Python** — analyse statistique (FFT, décomposition saisonnière)
- **Looker Studio** — visualisation et dashboard
- **Google Cloud** — infrastructure

## Ce que j'ai fait

### 1. Nettoyage des données
Normalisation de 10 ans de données brutes de marchés publics :
nettoyage des colonnes, création de tables de référence,
normalisation des catégories.

### 2. Analyse de corrélation
Corrélation entre le volume d'annonces et les indices économiques
BT01 (bâtiment) et PT01 (travaux publics), ainsi qu'avec les
cycles électoraux (présidentielles, municipales).

### 3. Détection de cycles saisonniers
Algorithme de détection automatique de cycles via FFT
(Fast Fourier Transform) + validation par décomposition saisonnière.
Score de fiabilité de 0 à 1. 137 segments cycliques identifiés
sur 5 dimensions croisées.

### 4. Dashboard Looker Studio
Visualisation des chiffres clés des 300 000 annonces sur 10 ans 
Visualisation des cycles par segment avec axe temporel commun
pour comparer les courbes entre types de marchés.

## Résultats clés
- Cycle annuel identifié sur le segment Logement/Bâtiment/MOP
  avec un score de fiabilité de **0.854**
- 62 segments à cycle annuel, 41 à cycle bi-annuel,
  24 à cycle tri-annuel
- Pic dominant en **Q1 (janvier-mars)** sur 55% des segments

## Structure du repo
