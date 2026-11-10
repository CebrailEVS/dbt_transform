
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_emplacement_deduit
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_machine_fin_mois`
where is_emplacement_deduit is null



  
  
      
    ) dbt_internal_test