
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select annee
from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`
where annee is null



  
  
      
    ) dbt_internal_test