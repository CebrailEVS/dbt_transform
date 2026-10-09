
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select no_ref_det
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_nomenclat`
where no_ref_det is null



  
  
      
    ) dbt_internal_test