
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vehicule_code
from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__vehicule_roadman`
where vehicule_code is null



  
  
      
    ) dbt_internal_test