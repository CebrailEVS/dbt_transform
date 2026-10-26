
    
    

with all_values as (

    select
        client_category as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by client_category

)

select *
from all_values
where value_field not in (
    'PREMIUM','VIP','OR','ARGENT','BRONZE'
)


