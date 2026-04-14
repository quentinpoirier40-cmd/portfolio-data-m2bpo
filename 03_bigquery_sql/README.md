# 03_bigquery_sql

Ensemble des vues et tables matérialisées créées dans BigQuery pour l'analyse.

## Structure

- `vues/tenders_final_clean_M2BPO.sql` : vue principale nettoyée (jointures, calculs, segments)
- `vues/VUE_LOOKER_M2BPO_INDICES.sql` : jointure avec les indices BT01/PT01 + calcul montant actualisé
- `vues/v_synthese_trimestrielle_annonces.sql` : agrégation par trimestre (volume, top procédure, top nature)
- `vues/creation_table_ville.sql` : table géographique (ville, département, région, ISO)
- `enrichissement/table_indices_BT01_PT01.sql` : création de la table des indices INSEE
- `enrichissement/table_departement_iso.sql` : ajout du code ISO aux départements
- `enrichissement/tables_flat_competences_docs_equipe.sql` : parsing des listes

## Ordre d'exécution recommandé

1. `table_indices_BT01_PT01.sql`
2. `table_departement_iso.sql`
3. `tables_flat_...sql`
4. `creation_table_ville.sql`
5. `tenders_final_clean_M2BPO.sql`
6. `VUE_LOOKER_M2BPO_INDICES.sql`
7. `v_synthese_trimestrielle_annonces.sql`
