
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select retard_delai
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__maintenance_preventive`
where retard_delai is null



  
  
      
    ) dbt_internal_test