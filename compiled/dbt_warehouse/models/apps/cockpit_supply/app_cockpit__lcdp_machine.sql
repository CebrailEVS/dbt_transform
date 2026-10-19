

-- Référentiel des machines Cafés du Phare pour Cockpit Supply : comptage par approvisionneur et catégorie, et
-- écran Équipements en stock (dépôt, n° de série, propriétaire, achat). Toutes les machines, sans filtre.
select
    device_id,
    device_code,
    device_name,
    device_serial_number,
    company_code,
    company_name,
    owner_company_name,
    device_brand,
    device_category,
    device_state,
    device_material_status,
    device_comment,
    assigned_roadman_code,
    is_active,
    purchase_date,
    purchase_cost
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`