
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activite_date
from `evs-datastack-prod`.`prod_marts`.`fct_bi__activite_rapport_jour`
where activite_date is null



  
  
      
    ) dbt_internal_test