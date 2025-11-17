
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ct_intitule
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptet`
where ct_intitule is null



  
  
      
    ) dbt_internal_test