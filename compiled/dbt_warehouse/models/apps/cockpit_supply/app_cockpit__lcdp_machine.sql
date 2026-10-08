

-- Référentiel des machines Cafés du Phare (comptage par approvisionneur et catégorie), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_id,
    is_active,
    assigned_roadman_code,
    device_category
from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`