
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_bi__usage_rapport`

where not(is_dormant = (nb_consultations = 0))


  
  
      
    ) dbt_internal_test