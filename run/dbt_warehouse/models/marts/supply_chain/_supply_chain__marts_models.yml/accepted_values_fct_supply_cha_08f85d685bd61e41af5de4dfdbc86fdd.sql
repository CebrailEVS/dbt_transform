
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        source_sigma as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`
    group by source_sigma

)

select *
from all_values
where value_field not in (
    'erreur','demande_fallback'
)



  
  
      
    ) dbt_internal_test