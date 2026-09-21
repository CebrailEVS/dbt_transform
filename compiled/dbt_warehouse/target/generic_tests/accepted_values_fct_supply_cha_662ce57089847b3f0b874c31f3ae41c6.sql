
    
    

with all_values as (

    select
        alerte_exploit as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
    group by alerte_exploit

)

select *
from all_values
where value_field not in (
    'arrete_mais_vend','actif_mais_silencieux'
)


