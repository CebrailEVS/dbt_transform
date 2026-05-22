
    
    

with all_values as (

    select
        product_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__consommation`
    group by product_type

)

select *
from all_values
where value_field not in (
    'THE','CAFE CAPS','CHOCOLATS VAN HOUTEN','BOISSONS GOURMANDES','ACCESSOIRES','CAFENOIR','INDEFINI','SNACKING','BOISSONS FRAICHES'
)


