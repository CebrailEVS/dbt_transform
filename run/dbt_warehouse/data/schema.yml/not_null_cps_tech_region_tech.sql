
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region_tech
from `evs-datastack-prod`.`prod_reference`.`cps_tech`
where region_tech is null



  
  
      
    ) dbt_internal_test