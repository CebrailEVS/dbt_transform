
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        type_commande as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_commande_delai`
    group by type_commande

)

select *
from all_values
where value_field not in (
    'INTERNE','CLIENT'
)



  
  
      
    ) dbt_internal_test