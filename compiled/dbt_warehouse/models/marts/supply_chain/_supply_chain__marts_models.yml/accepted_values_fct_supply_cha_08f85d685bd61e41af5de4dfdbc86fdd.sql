
    
    

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


