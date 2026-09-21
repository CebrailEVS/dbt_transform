
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select current_material_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__machine_appro_intervention`
    where current_material_id is not null
),

parent as (
    select material_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__material`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test