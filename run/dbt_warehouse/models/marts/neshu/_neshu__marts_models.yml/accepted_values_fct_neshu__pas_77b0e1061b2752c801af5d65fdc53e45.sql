
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_anomaly as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__passage_appro`
    group by is_anomaly

)

select *
from all_values
where value_field not in (
    0,1
)



  
  
      
    ) dbt_internal_test