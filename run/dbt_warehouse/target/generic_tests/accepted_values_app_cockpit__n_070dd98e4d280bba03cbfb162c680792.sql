
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        motif_exclusion as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_commande_delai`
    group by motif_exclusion

)

select *
from all_values
where value_field not in (
    'ANNULEE'
)



  
  
      
    ) dbt_internal_test