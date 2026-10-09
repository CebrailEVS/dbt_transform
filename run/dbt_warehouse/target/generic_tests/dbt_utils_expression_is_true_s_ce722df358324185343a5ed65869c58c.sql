
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_rw`

where not(bad_intervention_id != new_intervention_id)


  
  
      
    ) dbt_internal_test