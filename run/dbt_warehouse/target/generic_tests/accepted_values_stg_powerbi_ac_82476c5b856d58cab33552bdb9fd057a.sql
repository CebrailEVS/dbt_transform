
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        target_storage_mode as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__datasets`
    group by target_storage_mode

)

select *
from all_values
where value_field not in (
    'Abf','PremiumFiles'
)



  
  
      
    ) dbt_internal_test