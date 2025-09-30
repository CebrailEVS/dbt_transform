
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idlocation_type
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company_has_location`
where idlocation_type is null



  
  
      
    ) dbt_internal_test