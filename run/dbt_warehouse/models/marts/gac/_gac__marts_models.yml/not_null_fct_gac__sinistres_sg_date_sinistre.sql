
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_sinistre
from `evs-datastack-prod`.`prod_marts`.`fct_gac__sinistres_sg`
where date_sinistre is null



  
  
      
    ) dbt_internal_test