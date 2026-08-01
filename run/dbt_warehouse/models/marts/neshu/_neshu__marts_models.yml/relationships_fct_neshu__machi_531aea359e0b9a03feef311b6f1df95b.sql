
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select past_technician_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__machine_appro_intervention`
    where past_technician_id is not null
),

parent as (
    select user_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test