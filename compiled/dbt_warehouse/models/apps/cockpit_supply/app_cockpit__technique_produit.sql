

-- Référentiel des articles TechCare (pièces interdites / obligatoires) pour Cockpit Supply, construit depuis le staging.
select
    product_id,
    product_code,
    product_name,
    is_forbidden_article,
    is_mandatory_article
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__products`