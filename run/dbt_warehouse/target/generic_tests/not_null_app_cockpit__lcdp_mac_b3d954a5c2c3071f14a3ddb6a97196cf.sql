
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_saisie_apres_fin_mois
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_machine_fin_mois`
where is_saisie_apres_fin_mois is null



  
  
      
    ) dbt_internal_test