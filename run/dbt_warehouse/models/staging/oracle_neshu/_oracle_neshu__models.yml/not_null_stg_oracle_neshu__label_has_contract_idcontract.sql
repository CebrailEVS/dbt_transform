
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcontract
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_contract`
where idcontract is null



  
  
      
    ) dbt_internal_test