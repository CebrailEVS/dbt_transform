
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_web__mouvements_stock`
where extracted_at is null



  
  
      
    ) dbt_internal_test