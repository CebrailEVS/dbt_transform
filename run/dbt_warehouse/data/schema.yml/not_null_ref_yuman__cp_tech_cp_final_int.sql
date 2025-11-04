
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cp_final_int
from `evs-datastack-prod`.`prod_reference`.`ref_yuman__cp_tech`
where cp_final_int is null



  
  
      
    ) dbt_internal_test