
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select piece_id
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__piece`
where piece_id is null



  
  
      
    ) dbt_internal_test