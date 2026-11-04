
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select segment_start_at
from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_lifecycle_segments`
where segment_start_at is null



  
  
      
    ) dbt_internal_test