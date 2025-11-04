
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select Montant
from `evs-datastack-prod`.`prod_reference`.`ref_yuman__tarification_clean`
where Montant is null



  
  
      
    ) dbt_internal_test