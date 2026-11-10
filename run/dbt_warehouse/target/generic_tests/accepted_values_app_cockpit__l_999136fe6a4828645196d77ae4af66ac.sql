
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        product_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_produit`
    group by product_type

)

select *
from all_values
where value_field not in (
    'PRODU','LIGNE_PRIX','MODELE'
)



  
  
      
    ) dbt_internal_test