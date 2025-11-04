
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select equipe
from `evs-datastack-prod`.`prod_reference`.`ref_yuman__tech_agence`
where equipe is null



  
  
      
    ) dbt_internal_test