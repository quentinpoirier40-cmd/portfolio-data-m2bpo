CREATE OR REPLACE TABLE `m2bpo-data-analyse.m2bpo_data.10yrM2BPO_ville_clean` AS
SELECT 
    -- 1. Identifiants
    t.ID AS id_annonce,
    
    -- 2. Localisation Nettoyée
    UPPER(TRIM(t.Pays)) AS pays_propre,
    UPPER(TRIM(t.region)) AS region_source,
    
    -- 3. Ville propre (Nettoyage des symboles et mise en majuscules)
    UPPER(TRIM(REGEXP_REPLACE(ville_individuelle, r'^[^a-zA-ZÀ-ÿ0-9]+', ''))) AS ville_propre,
    
    -- 4. Département et Région (Enrichis)
    t.departement AS code_departement,
    UPPER(TRIM(d.nom_departement)) AS departement_nom,
    UPPER(TRIM(d.nom_region)) AS region_officielle,
    
    -- 5. Champ Géo pour Looker
    CONCAT(
        UPPER(TRIM(REGEXP_REPLACE(ville_individuelle, r'^[^a-zA-ZÀ-ÿ0-9]+', ''))), 
        ' (', t.departement, '), FRANCE'
    ) AS ville_dept_geo

FROM 
    `m2bpo-data-analyse.m2bpo_data.10yrtenders_final_clean_M2BPO` AS t,
    UNNEST(SPLIT(t.`ville_exe`, ',')) AS ville_individuelle
LEFT JOIN 
    `m2bpo-data-analyse.m2bpo_data.departement_natif` AS d
ON 
    TRIM(CAST(t.departement AS STRING)) = TRIM(CAST(d.code_departement AS STRING))
WHERE 
    ville_individuelle IS NOT NULL 
    AND TRIM(ville_individuelle) != ''
    AND t.departement NOT IN ('99', '990', 'FRANCE', 'PAYS')
    
    -- On garde l'exception PARIS (75)
    AND (
      (UPPER(TRIM(ville_individuelle)) != UPPER(TRIM(d.nom_departement)) OR t.departement = '75')
      AND UPPER(TRIM(ville_individuelle)) != UPPER(TRIM(t.region))
      AND UPPER(TRIM(ville_individuelle)) != UPPER(TRIM(d.nom_region))
      AND UPPER(TRIM(ville_individuelle)) != UPPER(TRIM(t.Pays))
    )
