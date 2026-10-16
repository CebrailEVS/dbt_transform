
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        coeff as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_type`
    group by coeff

)

select *
from all_values
where value_field not in (
    -1,0,1
)



  
  
      
    ) dbt_internal_test