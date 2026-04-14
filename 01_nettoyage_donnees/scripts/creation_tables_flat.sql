CREATE OR REPLACE VIEW `m2bpo_data.tenders_final_clean_M2BPO` AS 

WITH base_data AS (
SELECT

  -- 1. RENOMMAGE DES COLONNES (Clean Names)

  ID,

  `État` AS etat,

  `Objet` AS objet,

  `Département` AS departement,

  `Région` AS region,

  `Ville exe` AS ville_exe,

  Pays,

  MO,

  `MO _type_`AS MO_type_,

  `Insérée le` AS inseree_le,

  `Année` AS annee,

  Mois,

  Semaine,

  `Date limite le` AS date_limite_le,

  `Année_14` AS annee_limite,

  Mois_15 AS mois_limite,

  Semaine_16 AS semaine_limite,

  `Procédure` AS procedure_annonce,

  Contrat,

  `Nature _cat__` AS Nature_cat_,

  Nature,

  `Marché _cat__` AS marche_cat,

  `Marché` AS marche,

  `Forme juridique` AS forme_juridique,

  `Exclusivité` AS exclusivite,

  Mandataire,

  `Compétences` AS competences,

  `Équipe` AS equipe,

  `Documents __ fournir` AS documents_a_fournir,

  `Informations particulières` AS informations_particulieres,

  `Documents _nombre_` AS document_nombre,

  `Nombre Q_R` AS nombre_q_r,

  ` Montant travaux ` AS montant_travaux_raw, -- On garde le raw au cas où

  Surface,

  ` Montant marché ` AS montant_marche_raw,



  -- 2. GÉOGRAPHIE : Sécurisation du département (format 01, 02...)



    CONCAT("FR-",

      CASE

        WHEN LENGTH(CAST(`Département` AS STRING)) <= 2 THEN LPAD(CAST(`Département` AS STRING), 2, '0')

        ELSE CAST(`Département` AS STRING)

      END

    ) AS dept_iso,



  -- 4. DATES & TRIMESTRES SANS ANNee (KPIs de temps)

  DATE_DIFF(`Date limite le`, `Insérée le`, DAY) AS delai_consultation_jours,

  EXTRACT(QUARTER FROM `Date limite le`) AS trimestre_lim_sans_annee,



    -- 4.1 DATES & TRIMESTRES (KPIs de temps)

  DATE_TRUNC(`Date limite le`, QUARTER) AS date_trimestre_bon_annee,

 

    -- 4.2 SEGMENT délais annonce

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



  -- 5. TRI DES MOIS (Numérique)

  CASE

    WHEN LOWER(TRIM(Mois)) = 'janvier' THEN 1 WHEN LOWER(TRIM(Mois)) = 'février' THEN 2

    WHEN LOWER(TRIM(Mois)) = 'mars' THEN 3 WHEN LOWER(TRIM(Mois)) = 'avril' THEN 4

    WHEN LOWER(TRIM(Mois)) = 'mai' THEN 5 WHEN LOWER(TRIM(Mois)) = 'juin' THEN 6

    WHEN LOWER(TRIM(Mois)) = 'juillet' THEN 7 WHEN LOWER(TRIM(Mois)) = 'août' THEN 8

    WHEN LOWER(TRIM(Mois)) = 'septembre' THEN 9 WHEN LOWER(TRIM(Mois)) = 'octobre' THEN 10

    WHEN LOWER(TRIM(Mois)) = 'novembre' THEN 11 WHEN LOWER(TRIM(Mois)) = 'décembre' THEN 12

  END AS mois_ins_num,



    -- 5. TRI DES MOIS LIMITE (Numérique)

  CASE

    WHEN LOWER(TRIM(Mois_15)) = 'janvier' THEN 1 WHEN LOWER(TRIM(Mois_15)) = 'février' THEN 2

    WHEN LOWER(TRIM(Mois_15)) = 'mars' THEN 3 WHEN LOWER(TRIM(Mois_15)) = 'avril' THEN 4

    WHEN LOWER(TRIM(Mois_15)) = 'mai' THEN 5 WHEN LOWER(TRIM(Mois_15)) = 'juin' THEN 6

    WHEN LOWER(TRIM(Mois_15)) = 'juillet' THEN 7 WHEN LOWER(TRIM(Mois_15)) = 'août' THEN 8

    WHEN LOWER(TRIM(Mois_15)) = 'septembre' THEN 9 WHEN LOWER(TRIM(Mois_15)) = 'octobre' THEN 10

    WHEN LOWER(TRIM(Mois_15)) = 'novembre' THEN 11 WHEN LOWER(TRIM(Mois_15)) = 'décembre' THEN 12

  END AS mois_Lim_num,



  -- 6. FINANCES (Conversion FLOAT pour calculs)

  SAFE_CAST(` Montant travaux ` AS FLOAT64) AS montant_travaux_num,

  SAFE_CAST(` Montant marché ` AS FLOAT64) AS montant_marche_num,

  SAFE_CAST(Surface AS FLOAT64) AS surface_num,



  -- 7. NORMALISATION RÉGIONS (ISO)

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



  -- Calcul du nombre de compétences

    CASE

        WHEN `Compétences` IS NULL OR TRIM(`Compétences`) = '' THEN 0

        ELSE ARRAY_LENGTH(SPLIT(`Compétences`, '//'))

    END AS nb_competences_count,

   

    -- Calcul du nombre de documents

    CASE

        WHEN `Documents __ fournir` IS NULL OR TRIM(`Documents __ fournir`) = '' THEN 0

        ELSE ARRAY_LENGTH(SPLIT(`Documents __ fournir`, '//'))

    END AS nb_documents_count



FROM `m2bpo_data.tenders_rawV2`



-- 8. FILTRAGE DES DONNÉES POLLUÉES

WHERE NOT REGEXP_CONTAINS(

    UPPER(IFNULL(Objet, '')),

    r'^(ANNUL|DECLAREE[\W_]+SANS[\W_]+SUITE|DÉCLARATION[\W_]+SANS[\W_]+SUITE|MARCHE[\W_]+SANS[\W_]+SUITE|PROCÉDURE[\W_]+ANNULÉE|INFRUCT|MARCHE[\W_]+ANNULE|DOUBLON|AVIS[\W_]+INFRUCT|AVIS[\W_]+SANS[\W_]+SUITE|HORS[\W_]+SUJET|SANS[\W_]+SUITE)'

  )
)

-- 10. COUCHE FINALE : QUINTILES DYNAMIQUES
SELECT 
  *,
  -- Création de la répartition en 5 tranches égales (20% chacune)
  NTILE(5) OVER(
    PARTITION BY annee, procedure_annonce, Nature_cat_ 
    ORDER BY delai_consultation_jours
  ) AS quintile_delai_dynamique
FROM base_data;
