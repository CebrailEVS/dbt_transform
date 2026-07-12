
    
    

with all_values as (

    select
        methode_prevision as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
    group by methode_prevision

)

select *
from all_values
where value_field not in (
    'moyenne_mobile','moyenne_mobile_surveille','reference_saisonniere','naif','exclu'
)


