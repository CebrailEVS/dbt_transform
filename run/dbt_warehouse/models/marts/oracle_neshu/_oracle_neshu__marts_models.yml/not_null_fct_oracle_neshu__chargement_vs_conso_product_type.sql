
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_type
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__chargement_vs_conso`
where product_type is null



  
  
      
    ) dbt_internal_test