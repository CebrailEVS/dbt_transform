
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select employee_responsible
from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__opportunite`
where employee_responsible is null



  
  
      
    ) dbt_internal_test