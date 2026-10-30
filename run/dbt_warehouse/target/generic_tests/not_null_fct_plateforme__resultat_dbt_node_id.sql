
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select node_id
from `evs-datastack-prod`.`prod_marts`.`fct_plateforme__resultat_dbt`
where node_id is null



  
  
      
    ) dbt_internal_test