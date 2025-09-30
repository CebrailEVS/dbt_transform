
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idlabel
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_resources`
where idlabel is null



  
  
      
    ) dbt_internal_test