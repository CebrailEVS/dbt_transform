

WITH product_labels AS (
  SELECT 
    p.idproduct,
    p.idproduct_type,
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
  FROM `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` p
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_product` lhp 
    ON lhp.idproduct = p.idproduct
    AND lhp.idlabel IS NOT NULL
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` l 
    ON l.idlabel = lhp.idlabel
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` lf 
    ON lf.idlabel_family = l.idlabel_family
  WHERE p.idproduct_type IN (1, 5)
    AND (
      CASE 
        WHEN p.idproduct = 1 AND p.created_at IS NULL THEN p.updated_at
        ELSE p.created_at
      END
    ) IS NOT NULL
),

pivoted AS (
  SELECT *
  FROM product_labels
  PIVOT (
    MAX(label_code)
    FOR label_family_code IN (
      'MARQUEP' AS product_brand,
      'PROPRIETAIRE' AS product_owner,
      'FAMILLE' AS product_family, 
      'BIO' AS product_bio,
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
    product_brand,
    product_owner,
    product_family,
    product_bio,
    product_group,
    product_type_raw,
    -- convert isactive from string to boolean
    CASE
      WHEN LOWER(isactive) = 'yes' THEN TRUE
      ELSE FALSE
    END AS is_active,
    created_at,
    updated_at,
    -- logique de typologie standardisée
    COALESCE(
      CASE
        WHEN idproduct = 1 THEN 'INDEFINI'
        WHEN product_family IN ('CAFE CAPSULES', 'CAFE CAPSULES PREMIUM') THEN 'CAFE CAPS'
        WHEN product_family IN ('THE') THEN 'THE'
        WHEN product_group = 'ACCESSOIRES' THEN 'ACCESSOIRES'
        WHEN product_group = 'BOISSONS FRAICHES' THEN 'BOISSONS FRAICHES'
        WHEN product_group = 'SNACKING' THEN 'SNACKING'
        WHEN product_brand = 'VAN HOUTEN' AND product_code = 'VANHCHOC23' THEN 'CHOCOLATS VAN HOUTEN'
        WHEN product_type_raw = 'BGOURMANDE' THEN 'BOISSONS GOURMANDES'
        ELSE NULL
      END,
      NULLIF(TRIM(product_type_raw), ''),
      'Non renseigné'
    ) AS product_type
  FROM pivoted
),

standardized AS (
  SELECT
    idproduct,
    idproduct_type,
    product_code,
    product_name,
    purchase_unit_price,

    -- FORCING champs à 'INDEFINI' si product_type est 'INDEFINI'
    CASE WHEN product_type = 'INDEFINI' THEN 'INDEFINI' ELSE product_brand END AS product_brand,
    CASE WHEN product_type = 'INDEFINI' THEN 'INDEFINI' ELSE product_owner END AS product_owner,
    CASE WHEN product_type = 'INDEFINI' THEN 'INDEFINI' ELSE product_family END AS product_family,
    CASE WHEN product_type = 'INDEFINI' THEN 'INDEFINI' ELSE product_bio END AS product_bio,
    CASE WHEN product_type = 'INDEFINI' THEN 'INDEFINI' ELSE product_group END AS product_group,

    product_type_raw,
    is_active,
    created_at,
    updated_at,
    product_type
  FROM final
)

SELECT  
  idproduct,
  idproduct_type,
  product_code,
  product_name,
  purchase_unit_price,
  product_brand,
  product_owner,
  product_family,
  product_bio,
  product_group,
  product_type,
  is_active,
  created_at,
  updated_at
FROM standardized