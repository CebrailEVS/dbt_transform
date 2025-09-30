
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select Code_Postal
from `evs-datastack-prod`.`prod_reference`.`cp_metropole`
where Code_Postal is null



  
  
      
    ) dbt_internal_test