{{
  config(
    materialized='table',
    cluster_by=['idproduct'],
    description='Dimension produit enrichie à partir des labels associés (type, famille, groupe, marque, etc.) filtré sur les produits de type 1 (produit) et 5 (Ligne de prix).',
  )
}}

WITH product_labels AS (
  SELECT 
    p.idproduct,
    p.idproduct_type,
    p.code AS product_code,
    p.name AS product_name,
    p.purchase_unit_price AS purchase_unit_price,
    p.created_at,
    p.updated_at,
    l.code AS label_code,
    lf.code AS label_family_code
  FROM {{ ref('stg_oracle_neshu__product') }} p
  LEFT JOIN {{ ref('stg_oracle_neshu__label_has_product') }} lhp 
    ON lhp.idproduct = p.idproduct
    AND lhp.idlabel IS NOT NULL
  LEFT JOIN {{ ref('stg_oracle_neshu__label') }} l 
    ON l.idlabel = lhp.idlabel
  LEFT JOIN {{ ref('stg_oracle_neshu__label_family') }} lf 
    ON lf.idlabel_family = l.idlabel_family

  WHERE p.idproduct_type IN (1, 5)
),

pivoted AS (
  SELECT *
  FROM product_labels
  PIVOT (
    MAX(label_code)
    FOR label_family_code IN (
      'MARQUEP' AS brand,
      'PROPRIETAIRE' AS owner,
      'FAMILLE' AS family, 
      'BIO' AS bio,
      'GROUPE' AS product_group,
      'LPTYPE' AS product_type_raw,
      'ISACTIVE' AS isactive
    )
  )
),

final AS (
  SELECT
    idproduct,
    idproduct_type,
    product_code,
    product_name,
    purchase_unit_price,
    brand,
    owner,
    family,
    bio,
    product_group,
    product_type_raw,
    isactive,
    created_at,
    updated_at,
    -- logique de typologie standardisée
    COALESCE(
      CASE
        WHEN idproduct = 1 THEN 'INDEFINI'
        WHEN family IN ('CAFE CAPSULES', 'CAFE CAPSULES PREMIUM') THEN 'CAFE CAPS'
        WHEN family IN ('THE') THEN 'THE'
        WHEN product_group = 'ACCESSOIRES' THEN 'ACCESSOIRES'
        WHEN product_group = 'BOISSONS FRAICHES' THEN 'BOISSONS FRAICHES'
        WHEN product_group = 'SNACKING' THEN 'SNACKING'
        WHEN brand = 'VAN HOUTEN' AND product_code = 'VANHCHOC23' THEN 'CHOCOLATS VAN HOUTEN'
        WHEN product_type_raw = 'BGOURMANDE' THEN 'BOISSONS GOURMANDES'
        ELSE NULL
      END,
      NULLIF(TRIM(product_type_raw), ''),
      'Non renseigné'
    ) AS product_type_standard
  FROM pivoted
)

SELECT  
  idproduct,
  idproduct_type,
  product_code,
  product_name,
  purchase_unit_price,
  brand,
  owner,
  family,
  bio,
  product_group,
  product_type_standard,
  isactive,
  created_at,
  updated_at
FROM final
