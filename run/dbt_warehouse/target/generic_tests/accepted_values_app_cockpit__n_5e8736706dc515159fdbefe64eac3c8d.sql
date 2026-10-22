
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        categorie as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nayka_equipement_stock`
    group by categorie

)

select *
from all_values
where value_field not in (
    'Machine principale','Composant en stock','Composant rattaché','Terminal de paiement','À classer'
)



  
  
      
    ) dbt_internal_test