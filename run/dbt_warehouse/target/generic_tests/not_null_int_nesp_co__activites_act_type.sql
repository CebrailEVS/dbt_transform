
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select act_type
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__activites`
where act_type is null



  
  
      
    ) dbt_internal_test