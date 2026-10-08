

-- Référentiel des articles TechCare (pièces interdites / obligatoires), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    product_id,
    product_code,
    product_name,
    is_forbidden_article,
    is_mandatory_article
from `evs-datastack-prod`.`prod_marts`.`dim_technique__product`