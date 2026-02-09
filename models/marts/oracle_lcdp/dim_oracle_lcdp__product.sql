{{
  config(
    materialized='table',
    description='Dimension produit enrichie à partir des labels associés (famille, groupe, marque, bio, etc.) filtré sur les produits de type 1 (produit) et 5 (Ligne de prix).'
  )
}}

WITH product_labels AS (
  SELECT
    p.idproduct as product_id,
    p.idproduct_type as product_type_id,
    p.code AS product_code,
    p.name AS product_name,
    p.purchase_unit_price AS purchase_unit_price,
    -- Correction de created_at si idproduct = 1
    CASE
      WHEN p.idproduct = 1 AND p.created_at IS NULL THEN p.updated_at
      ELSE p.created_at
    END AS created_at,
    p.updated_at,
    l.code AS label_code,
    lf.code AS label_family_code
  FROM {{ ref('stg_oracle_lcdp__product') }} p
  LEFT JOIN {{ ref('stg_oracle_lcdp__label_has_product') }} lhp
    ON lhp.idproduct = p.idproduct
    AND lhp.idlabel IS NOT NULL
  LEFT JOIN {{ ref('stg_oracle_lcdp__label') }} l
    ON l.idlabel = lhp.idlabel
  LEFT JOIN {{ ref('stg_oracle_lcdp__label_family') }} lf
    ON lf.idlabel_family = l.idlabel_family
  WHERE p.idproduct_type IN (1, 5)
    AND (
      CASE
        WHEN p.idproduct = 1 AND p.created_at IS NULL THEN p.updated_at
        ELSE p.created_at
      END
    ) IS NOT NULL
),

aggregated_labels AS (
  SELECT
    product_id,
    product_type_id,
    product_code,
    product_name,
    purchase_unit_price,
    created_at,
    updated_at,
    MAX(CASE WHEN label_family_code = 'FAMIPRO' THEN label_code END) AS product_family,
    MAX(CASE WHEN label_family_code = 'GROUPRO' THEN label_code END) AS product_group,
    MAX(CASE WHEN label_family_code = 'BIO' THEN label_code END) AS product_bio,
    MAX(CASE WHEN label_family_code = 'ISACTIVE' THEN label_code END) AS is_active,
    MAX(CASE WHEN label_family_code = 'MARQPRO' THEN label_code END) AS product_brand
  FROM product_labels
  GROUP BY
    product_id,
    product_type_id,
    product_code,
    product_name,
    purchase_unit_price,
    created_at,
    updated_at
)

SELECT
  -- Identifiants
  product_id,
  product_type_id,

  -- Codes et noms
  product_code,
  product_name,
  purchase_unit_price,

  -- Caractéristiques produit
  product_family,
  product_group,
  product_brand,
  product_bio,

  CASE
    WHEN LOWER(is_active) = 'yes' THEN TRUE
    ELSE FALSE
  END AS is_active,

  -- Dates
  created_at,
  updated_at

FROM aggregated_labels
