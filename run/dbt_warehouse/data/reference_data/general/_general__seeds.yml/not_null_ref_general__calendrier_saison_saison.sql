
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select saison
from `evs-datastack-prod`.`prod_reference`.`ref_general__calendrier_saison`
where saison is null



  
  
      
    ) dbt_internal_test