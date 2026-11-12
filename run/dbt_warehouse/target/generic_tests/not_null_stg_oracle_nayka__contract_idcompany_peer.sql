
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcompany_peer
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__contract`
where idcompany_peer is null



  
  
      
    ) dbt_internal_test