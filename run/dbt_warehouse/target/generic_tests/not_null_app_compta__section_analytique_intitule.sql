
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select intitule
from `evs-datastack-prod`.`prod_app_compta`.`app_compta__section_analytique`
where intitule is null



  
  
      
    ) dbt_internal_test