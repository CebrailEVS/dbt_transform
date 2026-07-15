
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select bad_intervention_id
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_rw`
where bad_intervention_id is null



  
  
      
    ) dbt_internal_test