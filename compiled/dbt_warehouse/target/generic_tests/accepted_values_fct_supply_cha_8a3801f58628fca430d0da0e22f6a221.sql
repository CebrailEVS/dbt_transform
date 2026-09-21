
    
    

with all_values as (

    select
        saison_produit as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
    group by saison_produit

)

select *
from all_values
where value_field not in (
    'ete','hiver','annuel','non_renseigne'
)


