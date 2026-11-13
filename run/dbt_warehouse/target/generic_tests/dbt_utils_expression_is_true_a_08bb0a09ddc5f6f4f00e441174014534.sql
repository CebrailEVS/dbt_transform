
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`

where not((code_analytique is null) = (motif_a_categoriser is not null))


  
  
      
    ) dbt_internal_test