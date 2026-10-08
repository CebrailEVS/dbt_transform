{{ config(materialized='table') }}

-- Référentiel des articles Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    product_id,
    product_code,
    product_name,
    product_exploit,
    product_family,
    product_group,
    product_type,
    purchase_unit_price,
    product_classabc,
    product_planoete,
    product_planohiver,
    product_hpalme,
    product_bio,
    is_active
from {{ ref('dim_neshu__product') }}
