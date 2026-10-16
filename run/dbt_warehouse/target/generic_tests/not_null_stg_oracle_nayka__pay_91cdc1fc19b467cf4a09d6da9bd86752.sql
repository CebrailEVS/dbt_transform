
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idpayment_condition
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__payment_condition`
where idpayment_condition is null



  
  
      
    ) dbt_internal_test