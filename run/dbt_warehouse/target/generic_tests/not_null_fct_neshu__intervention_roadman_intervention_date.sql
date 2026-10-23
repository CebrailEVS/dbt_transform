
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select intervention_date
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_roadman`
where intervention_date is null



  
  
      
    ) dbt_internal_test