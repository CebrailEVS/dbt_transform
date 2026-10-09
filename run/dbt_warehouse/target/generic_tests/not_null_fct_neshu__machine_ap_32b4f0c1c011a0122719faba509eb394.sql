
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_appros_planifies_a_venir
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__machine_appro_intervention`
where nb_appros_planifies_a_venir is null



  
  
      
    ) dbt_internal_test