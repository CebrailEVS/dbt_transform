

-- Référentiel des techniciens TechCare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    user_id,
    user_name,
    entrepot_rattachement,
    storehouses_name,
    is_active,
    user_type
from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`