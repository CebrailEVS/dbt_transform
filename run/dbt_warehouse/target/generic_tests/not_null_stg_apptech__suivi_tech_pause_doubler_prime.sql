
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select doubler_prime
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_pause`
where doubler_prime is null



  
  
      
    ) dbt_internal_test