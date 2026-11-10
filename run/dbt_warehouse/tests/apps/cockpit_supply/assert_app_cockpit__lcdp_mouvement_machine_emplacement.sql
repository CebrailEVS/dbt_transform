
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
-- Machine dont l'emplacement actuel n'est pas la destination de son dernier mouvement validé : l'état d'un
-- équipement à une date, reconstitué à partir des mouvements, serait faux pour elle.
with dernier_mouvement as (
    select
        device_id,
        destination_code
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_mouvement_machine`
    where task_status_code in ('FAIT', 'VALIDE')
    qualify row_number() over (partition by device_id order by movement_at desc, task_id desc) = 1
)

select
    m.device_id,
    m.company_code,
    d.destination_code
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_machine` as m
inner join dernier_mouvement as d
    on m.device_id = d.device_id
where
    m.device_type = 'MACHINE'
    and m.company_code is distinct from d.destination_code
  
  
      
    ) dbt_internal_test