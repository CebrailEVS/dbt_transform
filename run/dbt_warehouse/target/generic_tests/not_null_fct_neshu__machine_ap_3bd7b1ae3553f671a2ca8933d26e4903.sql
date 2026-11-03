
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_appros_realises_90d
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__machine_appro_intervention`
where nb_appros_realises_90d is null



  
  
      
    ) dbt_internal_test