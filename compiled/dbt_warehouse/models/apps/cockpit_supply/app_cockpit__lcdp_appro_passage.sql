

-- Passages des approvisionneurs Cafés du Phare Distribution Auto, pour Cockpit Supply : seules les colonnes lues par l'app,
-- passages validés (FAIT, VALIDE) avec approvisionneur, sur les bornes de dates de l'app.
select
    task_id,
    roadman_code,
    task_start_date,
    task_status_code
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__appro_tasks_enriched`
where task_status_code in ('FAIT', 'VALIDE') and roadman_code is not null and task_start_date is not null and task_start_date >= timestamp('2026-01-01')