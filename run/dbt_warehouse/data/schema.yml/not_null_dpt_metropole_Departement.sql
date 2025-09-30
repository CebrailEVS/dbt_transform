
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select Departement
from `evs-datastack-prod`.`prod_reference`.`dpt_metropole`
where Departement is null



  
  
      
    ) dbt_internal_test