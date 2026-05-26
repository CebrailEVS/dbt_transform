
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ea_ligne
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
where ea_ligne is null



  
  
      
    ) dbt_internal_test