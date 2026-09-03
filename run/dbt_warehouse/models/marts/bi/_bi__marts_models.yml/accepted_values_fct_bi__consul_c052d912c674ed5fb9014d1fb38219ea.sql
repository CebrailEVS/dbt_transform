
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        user_domain as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_bi__consultation`
    group by user_domain

)

select *
from all_values
where value_field not in (
    'evs-pro.com','neshu.com'
)



  
  
      
    ) dbt_internal_test