
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select destination_code
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__commande_interne`
where destination_code is null



  
  
      
    ) dbt_internal_test