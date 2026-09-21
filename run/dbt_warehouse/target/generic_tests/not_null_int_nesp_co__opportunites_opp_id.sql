
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select opp_id
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__opportunites`
where opp_id is null



  
  
      
    ) dbt_internal_test