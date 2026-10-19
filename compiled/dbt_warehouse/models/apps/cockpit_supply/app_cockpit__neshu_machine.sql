

-- Référentiel des machines Neshu pour Cockpit Supply : préventives TechCare (actives ou non) et écran
-- Équipements en stock (dépôt, n° de série, propriétaire, achat, réservation). Toutes les machines, sans filtre.
select
    device_id,
    device_code,
    device_name,
    device_serial_number,
    company_code,
    company_name,
    owner_company_name,
    device_brand,
    device_gamme,
    device_category,
    device_state,
    device_material_status,
    device_comment,
    is_active,
    is_reserved,
    purchase_date,
    purchase_cost
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`