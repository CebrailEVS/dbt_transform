
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_events`

where not(periode = format('%04d-%02d', annee, mois))


  
  
      
    ) dbt_internal_test