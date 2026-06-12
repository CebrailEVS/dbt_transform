
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        statut as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__couverture_stock_neshu`
    group by statut

)

select *
from all_values
where value_field not in (
    'RUPTURE TOTALE','RUPTURE','VIGILANCE','OK','NON CONSOMME'
)



  
  
      
    ) dbt_internal_test