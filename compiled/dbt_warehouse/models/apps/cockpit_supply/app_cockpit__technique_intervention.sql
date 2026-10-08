

-- Interventions TechCare utiles à l'app : réalisées (onglet Clients, préventives
-- réalisées) et préventives planifiées ; 10 colonnes sur 58 (sans le texte libre).
select
    material_id,
    material_serial_number,
    workorder_category,
    client_name,
    site_name,
    machine_clean,
    workorder_type_grouped,
    intervention_state,
    date_done,
    date_planned
from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
where
    (intervention_state = 'REALISEE' and date_done is not null)
    or (workorder_type_grouped = 'Preventive' and intervention_state = 'PLANIFIEE')