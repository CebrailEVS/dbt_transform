

-- Référentiel des articles Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    product_id,
    product_code,
    product_name,
    product_family,
    product_group,
    product_bio,
    is_active
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__product`