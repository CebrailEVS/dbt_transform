
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        billing_validation_status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
    group by billing_validation_status

)

select *
from all_values
where value_field not in (
    'VALIDATED','MISSING_TARIF','UNTRACKABLE','NOT_BILLABLE'
)



  
  
      
    ) dbt_internal_test