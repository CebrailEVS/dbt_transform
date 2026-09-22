
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dbt_updated_at
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__consommation`
where dbt_updated_at is null



  
  
      
    ) dbt_internal_test