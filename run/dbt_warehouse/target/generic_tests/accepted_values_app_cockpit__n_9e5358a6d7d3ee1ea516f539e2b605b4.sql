
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        ligne_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_equipement_stock`
    group by ligne_type

)

select *
from all_values
where value_field not in (
    'equipement','commande'
)



  
  
      
    ) dbt_internal_test