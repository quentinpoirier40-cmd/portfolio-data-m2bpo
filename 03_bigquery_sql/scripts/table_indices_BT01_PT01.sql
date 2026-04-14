CREATE OR REPLACE VIEW `m2bpo-data-analyse.m2bpo_data.VUE_LOOKER_M2BPO_INDICES` AS
WITH derniers_indices AS (
    -- On récupère la valeur la plus récente de ta table d'indices pour l'actualisation à date
    SELECT 
        INDICEBT01 as dernier_BT, 
        INDICEPT01 as dernier_PT
    FROM `m2bpo-data-analyse.m2bpo_data.BT01PT01`
    ORDER BY MOISAN DESC
    LIMIT 1
)
SELECT 
    t.*,
    i.INDICEBT01 as indice_BT01_mensuel,
    i.INDICEPT01 as indice_PT01_mensuel,
    
    -- Calcul du montant actualisé selon la catégorie de l'annonce
    CASE 
        -- 1. Cas du Bâtiment (Indice BT01)
        WHEN t.nature_cat_ IN ('Bureaux', 'Logement', 'Patrimoine', 'Santé', 'Enseignement', 'Sport', 'Industrie', 'Commerce', 'Hôtellerie-Restauration', 'Culture', 'Sécurité') 
             OR t.marche_cat = 'Batiment'
        THEN ROUND(t.montant_travaux_num * (di.dernier_BT / NULLIF(i.INDICEBT01, 0)), 2)
        
        -- 2. Cas de l'Urbanisme / Infra (Indice PT01)
        WHEN t.nature_cat_ IN ('Ville et territoire', 'Hydraulique', 'Infrastructure', 'Mobilité')
             OR t.marche_cat = 'urbanisme'
        THEN ROUND(t.montant_travaux_num * (di.dernier_PT / NULLIF(i.INDICEPT01, 0)), 2)
        
        -- Par défaut, si pas de catégorie, on garde le montant initial
        ELSE t.montant_travaux_num 
    END AS montant_travaux_actualise

FROM 
    `m2bpo-data-analyse.m2bpo_data.tenders_final_clean_M2BPO` AS t
LEFT JOIN 
    `m2bpo-data-analyse.m2bpo_data.BT01PT01` AS i 
    -- Jointure cruciale : on arrondit la date d'insertion au 1er du mois pour matcher la table BT01
    ON DATE_TRUNC(t.inseree_le, MONTH) = i.MOISAN
CROSS JOIN 
    derniers_indices AS di
