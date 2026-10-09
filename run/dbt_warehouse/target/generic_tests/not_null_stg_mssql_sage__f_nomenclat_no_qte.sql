
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select no_qte
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_nomenclat`
where no_qte is null



  
  
      
    ) dbt_internal_test