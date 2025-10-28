
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select code_analytique
from `evs-datastack-prod`.`prod_reference`.`mapping_code_analytique__bu`
where code_analytique is null



  
  
      
    ) dbt_internal_test