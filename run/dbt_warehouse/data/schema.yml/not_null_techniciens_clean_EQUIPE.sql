
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EQUIPE
from `evs-datastack-prod`.`prod_reference`.`techniciens_clean`
where EQUIPE is null



  
  
      
    ) dbt_internal_test