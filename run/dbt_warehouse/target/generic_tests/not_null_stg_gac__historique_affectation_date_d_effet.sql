
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_d_effet
from `evs-datastack-prod`.`prod_staging`.`stg_gac__historique_affectation`
where date_d_effet is null



  
  
      
    ) dbt_internal_test