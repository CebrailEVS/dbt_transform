
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select n_planning as from_field
    from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_alerte_conso_aguila`
    where n_planning is not null
),

parent as (
    select n_planning as to_field
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test