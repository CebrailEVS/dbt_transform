
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcurrency
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__currency`
where idcurrency is null



  
  
      
    ) dbt_internal_test