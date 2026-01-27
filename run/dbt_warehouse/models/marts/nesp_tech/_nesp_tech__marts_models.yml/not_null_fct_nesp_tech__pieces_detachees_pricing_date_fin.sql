
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_fin
from `evs-datastack-prod`.`prod_marts`.`fct_nesp_tech__pieces_detachees_pricing`
where date_fin is null



  
  
      
    ) dbt_internal_test