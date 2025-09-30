
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cp_depart_int
from `evs-datastack-prod`.`prod_reference`.`cps_tech`
where cp_depart_int is null



  
  
      
    ) dbt_internal_test