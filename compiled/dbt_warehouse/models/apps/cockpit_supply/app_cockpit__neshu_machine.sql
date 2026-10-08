

-- Référentiel des machines Neshu (actives ou non, pour les préventives TechCare), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_id,
    device_code,
    is_active,
    company_name
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`