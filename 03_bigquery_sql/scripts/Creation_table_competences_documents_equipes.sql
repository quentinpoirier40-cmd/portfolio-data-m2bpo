CREATE OR REPLACE TABLE `m2bpo-data-analyse.m2bpo_data.10yrM2BPO_flat_competences_` AS
SELECT 
    t.ID as id_annonce,
    -- Si c'est vide, on peut mettre "Non renseigné" pour que ce soit propre dans Looker
    IFNULL(TRIM(comp_split), "Non renseigné") AS competences_label
FROM 
    `m2bpo-data-analyse.m2bpo_data.10yrtenders_final_clean_M2BPO` AS t
LEFT JOIN 
    UNNEST(SPLIT(t.Competences, '//')) AS comp_split
WHERE 
    -- On garde tout, mais on évite juste les chaînes vides issues du split
    (comp_split IS NULL OR TRIM(comp_split) != '')
--------------------------------------------------
CREATE OR REPLACE TABLE `m2bpo-data-analyse.m2bpo_data.10yrM2BPO_flat_documentsafournir_` AS
SELECT 
    t.ID as id_annonce,
    -- On affiche "Non renseigné" plutôt que du vide pour que ce soit propre dans Looker
    IFNULL(NULLIF(TRIM(doc), ''), "Non renseigné") AS documents_label
FROM 
    `m2bpo-data-analyse.m2bpo_data.10yrtenders_final_clean_M2BPO` AS t
LEFT JOIN 
    UNNEST(SPLIT(t.Documents_a_fournir, '//')) AS doc
-- On ne met pas de WHERE ici pour ne rien supprimer par erreur
----------------------------------------------------
CREATE OR REPLACE TABLE `m2bpo-data-analyse.m2bpo_data.10yrM2BPO_flat_équipes_` AS
SELECT 
    t.ID as id_annonce,
    -- On affiche "Non renseigné" plutôt que du vide pour que ce soit propre dans Looker
    IFNULL(NULLIF(TRIM(equipe), ''), "Non renseigné") AS equipe
FROM 
    `m2bpo-data-analyse.m2bpo_data.10yrtenders_final_clean_M2BPO` AS t
LEFT JOIN 
    UNNEST(SPLIT(t.equipe, '//')) AS equipe
-- On ne met pas de WHERE ici pour ne rien supprimer par erreur
