
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
-- Tâche de fabrication validée des 35 derniers jours saisie pour un simple transfert (sortie qui ne part pas de
-- FABRICATION, entrée qui n'est pas VERT → FABRICATION) : comptée en livraison interne, à signaler à la torréfaction.
select
    'SORTIE_FABRICATION' as type_tache,
    task_id,
    source_code,
    destination_code,
    task_start_date
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__sortie_fabrication_tasks`
where
    is_transfert
    and task_status_code in ('FAIT', 'VALIDE')
    and task_start_date >= timestamp_sub(current_timestamp(), interval 35 day)

union all

select
    'ENTREE_FABRICATION' as type_tache,
    task_id,
    source_code,
    destination_code,
    task_start_date
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__entree_fabrication_tasks`
where
    is_transfert
    and task_status_code in ('FAIT', 'VALIDE')
    and task_start_date >= timestamp_sub(current_timestamp(), interval 35 day)
  
  
      
    ) dbt_internal_test