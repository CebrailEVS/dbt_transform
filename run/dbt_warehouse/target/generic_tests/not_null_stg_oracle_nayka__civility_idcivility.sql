
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcivility
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__civility`
where idcivility is null



  
  
      
    ) dbt_internal_test