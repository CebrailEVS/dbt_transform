
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select third
from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__client`
where third is null



  
  
      
    ) dbt_internal_test