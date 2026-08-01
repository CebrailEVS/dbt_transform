
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_appros_planifies_a_venir
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_machine_context`
where nb_appros_planifies_a_venir is null



  
  
      
    ) dbt_internal_test