
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        n_planning, etat_intervention, date_heure_fin
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions`
    group by n_planning, etat_intervention, date_heure_fin
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test