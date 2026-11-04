
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quantite
from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique`
where quantite is null



  
  
      
    ) dbt_internal_test