
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcompany
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
where idcompany is null



  
  
      
    ) dbt_internal_test