
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TECHNICIEN
from `evs-datastack-prod`.`prod_reference`.`ref_yuman__technicien_clean`
where TECHNICIEN is null



  
  
      
    ) dbt_internal_test