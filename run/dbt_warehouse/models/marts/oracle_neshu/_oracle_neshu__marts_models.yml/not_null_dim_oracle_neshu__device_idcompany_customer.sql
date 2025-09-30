
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcompany_customer
from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device`
where idcompany_customer is null



  
  
      
    ) dbt_internal_test