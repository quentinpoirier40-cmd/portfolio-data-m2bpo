CREATE OR REPLACE VIEW `m2bpo_data.10yrtenders_final_clean_M2BPO` AS 

WITH base_data AS (
SELECT
  ID,
  `État` AS etat,
  `Objet` AS objet,
  
  -- 1. Gestion du département
  CASE 
    WHEN CAST(`Département` AS STRING) = 'ZZZ' THEN '99' 
    ELSE CAST(`Département` AS STRING) 
  END AS departement,
  
  `Région` AS region,
  `Ville exe` AS ville_exe,
  
  -- 2. Typologie Pays
  CASE 
    WHEN CAST(`Département` AS STRING) = 'ZZZ' THEN 'Étranger' 
    ELSE 'France' 
  END AS Pays,

  MO,
  `MO _type_` AS MO_type_,
  `Insérée le` AS inseree_le,
  `Année` AS annee,
  Mois,
  Semaine,
  `Date limite le` AS date_limite_le,
  `Année_13` AS annee_limite,
  Mois_14 AS mois_limite,
  Semaine_15 AS semaine_limite,
  `Procédure` AS procedure_annonce,

  -- NORMALISATION CONTRAT (Demande spécifique)
  CASE 
    WHEN TRIM(Contrat) IN ('Base loi MOP', 'Mission de Base') THEN 'Base loi MOP et Mission de Base'
    ELSE Contrat 
  END AS contrat_normalise,

  `Nature _cat__` AS Nature_cat_,
  Nature,
  `Marché _cat__` AS marche_cat,
  `Marché` AS marche,
  `Forme juridique` AS forme_juridique,
  `Exclusivité` AS exclusivite,
  Mandataire,
  `Compétences` AS competences,
  `Équipe` AS equipe,
  `Documents à fournir` AS documents_a_fournir,
  `Informations particulières` AS informations_particulieres,
  `Documents _nombre_` AS document_nombre,
  `Nombre Q_R` AS nombre_q_r,
  SAFE_CAST(`Montant travaux` AS FLOAT64) AS montant_travaux_num,
  SAFE_CAST(Surface AS FLOAT64) AS surface_num,
  SAFE_CAST(`Montant marché` AS FLOAT64) AS montant_marche_num,

  -- GÉOGRAPHIE : ISO codes pour DataViz (Looker/PowerBI)
  CONCAT("FR-",
    CASE
      WHEN CAST(`Département` AS STRING) = 'ZZZ' THEN '99'
      WHEN LENGTH(CAST(`Département` AS STRING)) <= 2 THEN LPAD(CAST(`Département` AS STRING), 2, '0')
      ELSE CAST(`Département` AS STRING)
    END
  ) AS dept_iso,

  -- ANALYSE TEMPORELLE
  DATE_DIFF(`Date limite le`, `Insérée le`, DAY) AS delai_consultation_jours,
  EXTRACT(QUARTER FROM `Date limite le`) AS trimestre_lim_sans_annee,
  DATE_TRUNC(`Date limite le`, QUARTER) AS date_trimestre_bon_annee,

  -- SEGMENTATION DÉLAIS
  CASE
        WHEN DATE_DIFF(DATE(`Date limite le`), DATE(`Insérée le`), DAY) < 0 THEN "0. Erreur (Négatif)"
        WHEN DATE_DIFF(DATE(`Date limite le`), DATE(`Insérée le`), DAY) <= 10 THEN "1. 0-10 jours"
        WHEN DATE_DIFF(DATE(`Date limite le`), DATE(`Insérée le`), DAY) <= 20 THEN "2. 11-20 jours"
        WHEN DATE_DIFF(DATE(`Date limite le`), DATE(`Insérée le`), DAY) <= 30 THEN "3. 21-30 jours"
        WHEN DATE_DIFF(DATE(`Date limite le`), DATE(`Insérée le`), DAY) <= 40 THEN "4. 31-40 jours"
        WHEN DATE_DIFF(DATE(`Date limite le`), DATE(`Insérée le`), DAY) <= 50 THEN "5. 41-50 jours"
        WHEN DATE_DIFF(DATE(`Date limite le`), DATE(`Insérée le`), DAY) <= 60 THEN "6. 51-60 jours"
        ELSE "7. +60 jours"
  END AS segment_delais_annonce,

  -- NORMALISATION RÉGIONS ISO
  CASE
    WHEN `Région` IN ('Île-de-France', 'Ile-de-France', 'ILE DE FRANCE') THEN 'FR-IDF'
    WHEN `Région` = 'Aquitaine-Limousin-Poitou-Charentes' THEN 'FR-NAQ'
    WHEN `Région` = 'Languedoc-Roussillon-Midi-Pyrénées' THEN 'FR-OCC'
    WHEN `Région` = 'Alsace-Champagne-Ardenne-Lorraine' THEN 'FR-GES'
    WHEN `Région` = 'Nord-Pas-de-Calais-Picardie' THEN 'FR-HDF'
    WHEN `Région` = 'Auvergne-Rhône-Alpes' THEN 'FR-ARA'
    WHEN `Région` = "Provence-Alpes-Côte d'Azur" THEN 'FR-PAC'
    WHEN `Région` = 'Bretagne' THEN 'FR-BRE'
    WHEN `Région` = 'Pays de la Loire' THEN 'FR-PDL'
    WHEN `Région` = 'Normandie' THEN 'FR-NOR'
    WHEN `Région` = 'Bourgogne-Franche-Comté' THEN 'FR-BFC'
    WHEN `Région` = 'Centre-Val de Loire' THEN 'FR-CVL'
    WHEN `Région` = 'Corse' THEN 'FR-COR'
    ELSE `Région`
  END AS region_iso,

  -- FEATURES ENGINEERING : Parsing des chaînes concaténées
  CASE WHEN `Compétences` IS NULL OR TRIM(`Compétences`) = '' THEN 0 ELSE ARRAY_LENGTH(SPLIT(`Compétences`, '//')) END AS nb_competences_count,
  CASE WHEN `Documents à fournir` IS NULL OR TRIM(`Documents à fournir`) = '' THEN 0 ELSE ARRAY_LENGTH(SPLIT(`Documents à fournir`, '//')) END AS nb_documents_count

FROM `m2bpo-data-analyse.m2bpo_data.stg_10ansm2bpo-data-analyse`

-- FILTRE DE QUALITÉ : Exclusion des annulations et doublons au niveau de la vue
WHERE NOT REGEXP_CONTAINS(
    UPPER(IFNULL(Objet, '')),
    r'^(ANNUL|DECLAREE|DÉCLARATION|MARCHE SANS SUITE|PROCÉDURE ANNULÉE|INFRUCT|DOUBLON|HORS SUJET)'
  )
)

SELECT 
  *,
  -- Calcul de quintile pour identifier les appels d'offres "urgents" vs "standards" par catégorie
  NTILE(5) OVER(PARTITION BY annee, procedure_annonce, Nature_cat_ ORDER BY delai_consultation_jours) AS quintile_delai_dynamique
FROM base_data;
