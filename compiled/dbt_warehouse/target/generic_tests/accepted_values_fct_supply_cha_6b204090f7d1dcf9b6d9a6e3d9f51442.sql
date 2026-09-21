
    
    

with all_values as (

    select
        classe_abc as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
    group by classe_abc

)

select *
from all_values
where value_field not in (
    'A','B','C'
)


