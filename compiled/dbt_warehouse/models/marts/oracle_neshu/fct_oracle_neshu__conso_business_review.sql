

WITH telemetry_data AS (
  SELECT 
    -- IDs
    t.company_id,
    t.device_id,
    t.location_id,
    t.product_id,
    
    -- Company
    c.company_code,
    c.company_name,

    -- Localisation
    COALESCE(NULLIF(t.task_location_info, ''), d.device_location) AS location,

    -- Machine
    d.device_code AS device_serial_number,
    d.device_name,
    d.device_brand,
    d.device_economic_model,

    -- Produit
    p.product_name,
    p.product_brand,
    p.product_family,
    p.product_group,
    p.product_type,

    -- Contexte
    DATE(t.task_start_date) AS consumption_date,
    'TELEMETRIE' AS data_source,

    -- Mesure
    SUM(t.telemetry_quantity) AS quantity
    
  FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetry_tasks` t
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` d 
    ON t.device_id = d.device_id
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` p 
    ON t.product_id = p.product_id
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` c
    ON t.company_id = c.company_id
  GROUP BY 
    t.company_id, t.device_id, t.location_id, t.product_id,
    c.company_code, c.company_name,
    COALESCE(NULLIF(t.task_location_info, ''), d.device_location),
    d.device_code, d.device_name, d.device_brand, d.device_economic_model,
    p.product_name, p.product_brand, p.product_family, p.product_group, p.product_type,
    DATE(t.task_start_date)
),
chargement_data AS (
  SELECT 
    -- IDs
    l.company_id,
    l.device_id,
    l.location_id,
    l.product_id,
    
    -- Company
    c.company_code,
    c.company_name,

    -- Localisation
    COALESCE(NULLIF(l.task_location_info, ''), d.device_location) AS location,

    -- Machine
    d.device_code AS device_serial_number,
    d.device_name,
    d.device_brand,
    d.device_economic_model,

    -- Produit
    p.product_name,
    p.product_brand,
    p.product_family,
    p.product_group,
    p.product_type,

    -- Contexte
    DATE(l.task_start_date) AS consumption_date,
    'CHARGEMENT' AS data_source,

    -- Quantité (ajustée selon le produit)
    SUM(
      CASE
        WHEN p.product_name LIKE '%GOBELET%RAME 50%' THEN l.load_quantity * 50
        WHEN p.product_name LIKE '%GOBELET%RAME DE 30%' THEN l.load_quantity * 30
        WHEN p.product_name LIKE '%GOBELET%RAME 35%' THEN l.load_quantity * 35
        WHEN p.product_name LIKE '%MELANG%BTE 200%' THEN l.load_quantity * 200
        WHEN p.product_name LIKE '%MELANGEUR%BTE 200%' THEN l.load_quantity * 200
        WHEN p.product_name LIKE '%MELANGEUR%BTE 100%' THEN l.load_quantity * 100
        WHEN p.product_name LIKE '%BEGHIN SAY 300%' THEN l.load_quantity * 300
        WHEN p.product_name LIKE '%CARTON DE 500%' THEN l.load_quantity * 500
        WHEN p.product_name LIKE '%DISTRIBUTEUR 300 SUCRES%' THEN l.load_quantity * 300
        WHEN p.product_name LIKE '%SUCRE BATONNET 100%' THEN l.load_quantity * 100
        WHEN p.product_name LIKE '%SUCRE BTE 300%' THEN l.load_quantity * 300
        WHEN p.product_name LIKE '%NESPRESSO MELANGEURS EN BAMBOU INDI%' THEN l.load_quantity * 100
        ELSE l.load_quantity
      END
    ) AS quantity

  FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` l
  INNER JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` d 
    ON l.device_id = d.device_id
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` p 
    ON l.product_id = p.product_id
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` c
    ON l.company_id = c.company_id
  WHERE l.task_status_code in ('FAIT','VALIDE')
  GROUP BY 
    l.company_id, l.device_id, l.location_id, l.product_id,
    COALESCE(NULLIF(l.task_location_info, ''), d.device_location), c.company_code, c.company_name,
    d.device_code, d.device_name, d.device_brand, d.device_economic_model,
    p.product_name, p.product_brand, p.product_family, p.product_group, p.product_type,
    DATE(l.task_start_date)
),
livraison_data AS (
  SELECT
    -- IDs
    lt.company_id,
    NULL AS device_id,
    NULL AS location_id,
    lt.product_id,

    -- Company
    c.company_code,
    c.company_name,

    -- Localisation (fixe)
    'LIVRAISON' AS location,

    -- Machine (fixe)
    'LIVRAISON' AS device_serial_number,
    'LIVRAISON' AS device_name,
    'LIVRAISON' AS device_brand,
    'LIVRAISON' AS device_economic_model,

    -- Produit
    p.product_name,
    p.product_brand,
    p.product_family,
    p.product_group,
    p.product_type,

    -- Contexte
    DATE(lt.task_start_date) AS consumption_date,
    'LIVRAISON' AS data_source,

    -- Quantité (ajustée selon le produit)
    SUM(
      CASE
        WHEN p.product_name LIKE '%GOBELET%RAME 50%' THEN lt.quantity * 50
        WHEN p.product_name LIKE '%GOBELET%RAME DE 30%' THEN lt.quantity * 30
        WHEN p.product_name LIKE '%GOBELET%RAME 35%' THEN lt.quantity * 35
        WHEN p.product_name LIKE '%MELANG%BTE 200%' THEN lt.quantity * 200
        WHEN p.product_name LIKE '%MELANGEUR%BTE 200%' THEN lt.quantity * 200
        WHEN p.product_name LIKE '%MELANGEUR%BTE 100%' THEN lt.quantity * 100
        WHEN p.product_name LIKE '%BEGHIN SAY 300%' THEN lt.quantity * 300
        WHEN p.product_name LIKE '%CARTON DE 500%' THEN lt.quantity * 500
        WHEN p.product_name LIKE '%DISTRIBUTEUR 300 SUCRES%' THEN lt.quantity * 300
        WHEN p.product_name LIKE '%SUCRE BATONNET 100%' THEN lt.quantity * 100
        WHEN p.product_name LIKE '%SUCRE BTE 300%' THEN lt.quantity * 300
        WHEN p.product_name LIKE '%NESPRESSO MELANGEURS EN BAMBOU INDI%' THEN lt.quantity * 100
        ELSE lt.quantity
      END
    ) AS quantity

  FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_tasks` lt
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` p 
    ON lt.product_id = p.product_id
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` c
    ON lt.company_id = c.company_id
  WHERE lt.task_status_code in ('FAIT','VALIDE')
  AND p.product_type in ('THE','CAFE CAPS','CHOCOLATS VAN HOUTEN','BOISSONS GOURMANDES','ACCESSOIRES')
  GROUP BY 
    lt.company_id, lt.product_id,
    p.product_name, p.product_brand, p.product_family, p.product_group, p.product_type, c.company_code, c.company_name,
    DATE(lt.task_start_date)
),
-- Version optimisée : remplace combined_data + donnees_filtrees
combined_and_filtered_data AS (

  -- Cas particulier : machine AS00446 chez CN1046
  SELECT *
  FROM chargement_data
  WHERE device_serial_number = 'AS00446'
    AND company_code = 'CN1046'
    AND consumption_date < '2025-08-28'
    AND product_type IN ('THE','CAFE CAPS','CHOCOLATS VAN HOUTEN')

  UNION ALL

  SELECT *
  FROM telemetry_data
  WHERE device_serial_number = 'AS00446'
    AND company_code = 'CN1046'
    AND consumption_date >= '2025-08-28'

  UNION ALL

  -- TELEMETRIE avec filtres
  SELECT *
  FROM telemetry_data
  WHERE product_type IN ('BOISSONS GOURMANDES', 'CAFE CAPS', 'CAFENOIR', 'INDEFINI', 'THE', 'SNACKING', 'BOISSONS FRAICHES', 'CHOCOLATS VAN HOUTEN')
    AND NOT (
      device_brand IN ('NESTLE','ANIMO')
      AND (device_economic_model NOT IN ('Participatif valeurs','Participatif unités','Payant') OR device_economic_model IS NULL)
      AND product_type = 'THE'
    )
    AND NOT (company_code = 'CN1071' AND product_type = 'THE')
    AND NOT (device_serial_number = 'AS00446' AND company_code = 'CN1046') 


  UNION ALL

  -- CHARGEMENT avec tous les filtres consolidés
  SELECT *
  FROM chargement_data
  WHERE (
    (device_brand = 'NESPRESSO'
     AND (device_economic_model NOT IN ('Participatif valeurs','Participatif unités','Payant') OR device_economic_model IS NULL)
     AND product_type IN ('THE','CAFE CAPS')
     AND (company_code <> 'CN1070' OR consumption_date >= '2025-03-01'))
    OR (device_brand IN ('NESTLE','ANIMO')
        AND (device_economic_model NOT IN ('Participatif valeurs','Participatif unités','Payant') OR device_economic_model IS NULL)
        AND product_type = 'THE')
    OR (device_brand IN ('NESPRESSO','NESTLE','ANIMO')
        AND product_type = 'CHOCOLATS VAN HOUTEN')
    OR (product_type = 'ACCESSOIRES')
    OR (company_code = 'CN1071' AND product_type = 'THE')
    AND NOT (device_serial_number = 'AS00446' AND company_code = 'CN1046') 
  )

  UNION ALL

  -- LIVRAISON (tous les enregistrements)
  SELECT *
  FROM livraison_data
)


SELECT 
  -- Identifiants
  company_id,
  device_id,
  location_id,
  product_id,
  
  -- Company
  company_code,
  company_name,

  -- Localisation
  location,

  -- Machine
  device_serial_number,
  device_name,
  device_brand,
  device_economic_model,

  -- Produit
  product_name,
  product_brand,
  product_family,
  product_group,
  product_type,

  -- Contexte
  consumption_date,
  data_source,

  -- Mesure
  quantity,

  -- Métadonnées d'exécution
  CURRENT_TIMESTAMP() as dbt_updated_at,
  '95a87eb6-dc2f-477b-8adc-14a117128367' as dbt_invocation_id

FROM combined_and_filtered_data