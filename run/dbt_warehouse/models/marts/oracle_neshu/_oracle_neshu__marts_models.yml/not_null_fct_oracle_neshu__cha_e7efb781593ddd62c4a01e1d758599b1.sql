
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quinzaine_chgt
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__chargement_par_quinzaine`
where quinzaine_chgt is null



  
  
      
    ) dbt_internal_test