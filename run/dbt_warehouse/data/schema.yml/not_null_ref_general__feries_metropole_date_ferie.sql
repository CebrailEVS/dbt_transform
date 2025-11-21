
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_ferie
from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`
where date_ferie is null



  
  
      
    ) dbt_internal_test