
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mois
from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`
where mois is null



  
  
      
    ) dbt_internal_test