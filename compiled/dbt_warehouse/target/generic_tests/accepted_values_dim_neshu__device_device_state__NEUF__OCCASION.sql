
    
    

with all_values as (

    select
        device_state as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
    group by device_state

)

select *
from all_values
where value_field not in (
    'NEUF','OCCASION'
)


