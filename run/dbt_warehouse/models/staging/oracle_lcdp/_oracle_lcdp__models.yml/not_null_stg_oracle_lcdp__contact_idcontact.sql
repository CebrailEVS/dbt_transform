
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcontact
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact`
where idcontact is null



  
  
      
    ) dbt_internal_test