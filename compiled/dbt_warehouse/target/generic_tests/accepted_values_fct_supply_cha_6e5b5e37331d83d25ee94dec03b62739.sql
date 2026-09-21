
    
    

with all_values as (

    select
        alerte_saison as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
    group by alerte_saison

)

select *
from all_values
where value_field not in (
    'saison_hiver_mais_vend_ete','saison_ete_mais_vend_hiver'
)


