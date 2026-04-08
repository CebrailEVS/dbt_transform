
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select n_inter_6mois
from `evs-datastack-prod`.`prod_marts`.`fct_commerce__machines_avec_interventions`
where n_inter_6mois is null



  
  
      
    ) dbt_internal_test