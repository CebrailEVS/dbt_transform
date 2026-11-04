
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_piece_bl
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_reception`
where date_piece_bl is null



  
  
      
    ) dbt_internal_test