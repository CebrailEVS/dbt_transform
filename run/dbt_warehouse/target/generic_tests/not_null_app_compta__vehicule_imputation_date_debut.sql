
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_debut
from `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`
where date_debut is null



  
  
      
    ) dbt_internal_test