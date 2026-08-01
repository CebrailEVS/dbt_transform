
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select n_analytique
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
where n_analytique is null



  
  
      
    ) dbt_internal_test