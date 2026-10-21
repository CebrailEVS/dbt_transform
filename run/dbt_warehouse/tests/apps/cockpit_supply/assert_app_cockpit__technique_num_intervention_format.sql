
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- N° d'intervention Nomad hors 7 chiffres : Cockpit Supply rapproche une pose d'astreinte de sa régularisation
-- Yuman (« ASTREINTE <n°> ») sur ce n°. Rattaché aussi au dernier modèle de la chaîne : en échec, le job échoue
-- (alerte) sans qu'aucune table saute, dbt ne bloquant que les modèles en aval de tous les parents du test.
-- depends_on: `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_mensuelle`
select
    num_intervention,
    count(*) as nb_lignes
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_detail`
where not regexp_contains(num_intervention, r'^[0-9]{7}$')
group by num_intervention
  
  
      
    ) dbt_internal_test