
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        device_material_status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
    group by device_material_status

)

select *
from all_values
where value_field not in (
    'S'
)



  
  
      
    ) dbt_internal_test