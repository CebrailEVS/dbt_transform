
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_bi__usage_rapport`

where not(not is_dormant or derniere_consultation_at is null)


  
  
      
    ) dbt_internal_test