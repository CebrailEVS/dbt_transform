
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select de_intitule
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot`
where de_intitule is null



  
  
      
    ) dbt_internal_test