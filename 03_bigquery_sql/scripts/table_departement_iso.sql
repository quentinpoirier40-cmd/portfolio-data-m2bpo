UPDATE `m2bpo-data-analyse.m2bpo_data.departement_natif`
SET code_iso = CASE 
    -- Cas 1 : Codes à 1 chiffre (1 -> FR-01)
    WHEN LENGTH(TRIM(CAST(code_departement AS STRING))) = 1 
      THEN CONCAT('FR-', LPAD(TRIM(CAST(code_departement AS STRING)), 2, '0'))
    
    -- Cas 2 : Codes à 2 chiffres (75 -> FR-75)
    WHEN LENGTH(TRIM(CAST(code_departement AS STRING))) = 2 
      THEN CONCAT('FR-', TRIM(CAST(code_departement AS STRING)))
    
    -- Cas 3 : Codes à 3 chiffres (971 -> FR-971)
    WHEN LENGTH(TRIM(CAST(code_departement AS STRING))) = 3 
      THEN CONCAT('FR-', TRIM(CAST(code_departement AS STRING)))
    
    -- Par sécurité pour le reste (ex: 2A, 2B)
    ELSE CONCAT('FR-', TRIM(CAST(code_departement AS STRING)))
END
WHERE code_departement IS NOT NULL;
