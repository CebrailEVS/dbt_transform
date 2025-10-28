
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select designation_compte
from `evs-datastack-prod`.`prod_reference`.`mapping_code_comptable__bu`
where designation_compte is null



  
  
      
    ) dbt_internal_test