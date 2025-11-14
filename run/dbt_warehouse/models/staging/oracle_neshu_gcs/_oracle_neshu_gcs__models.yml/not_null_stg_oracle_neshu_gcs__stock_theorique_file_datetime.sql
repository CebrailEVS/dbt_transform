
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select file_datetime
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu_gcs__stock_theorique`
where file_datetime is null



  
  
      
    ) dbt_internal_test