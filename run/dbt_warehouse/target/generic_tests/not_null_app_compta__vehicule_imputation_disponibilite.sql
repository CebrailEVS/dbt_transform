
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select disponibilite
from `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`
where disponibilite is null



  
  
      
    ) dbt_internal_test