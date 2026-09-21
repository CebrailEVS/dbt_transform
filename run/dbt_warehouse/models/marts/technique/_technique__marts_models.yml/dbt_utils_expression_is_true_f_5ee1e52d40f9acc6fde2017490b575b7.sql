
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`

where not(not (
  tech_id_reel_astreinte is not null
  and tech_id_reel_modif is not null
  and tech_id_reel_astreinte != tech_id_reel_modif
))


  
  
      
    ) dbt_internal_test