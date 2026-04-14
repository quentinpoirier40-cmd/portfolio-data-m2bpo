# 01_nettoyage_donnees

Préparation des données brutes issues de M2BPO avant analyse.

## Contenu

- `scripts/nettoyage_encoding_doublons.py` : correction d'encodage (ISO-8859-1 → UTF-8), suppression des doublons, gestion des valeurs vides.
- `scripts/creation_tables_flat.sql` : parsing des colonnes multi-valeurs (compétences, documents, équipes) en tables "flat" pour Looker Studio.

## Résultats

- Nettoyage des ~500k annonces (2015-2024)
- Création des tables : `M2BPO_flat_competences_`, `M2BPO_flat_documentsafournir_`, `M2BPO_flat_équipes_`
- Création de la table géographique : `M2BPO_ville_clean`

## Exécution

1. Lancer le script Python sur les fichiers sources.
2. Exécuter les requêtes SQL dans BigQuery (ordre : tables flat, puis ville, puis vue finale).
