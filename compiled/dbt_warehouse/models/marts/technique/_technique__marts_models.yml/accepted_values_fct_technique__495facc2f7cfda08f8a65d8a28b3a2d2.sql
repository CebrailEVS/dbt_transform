
    
    

with all_values as (

    select
        workorder_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__workorder_product`
    group by workorder_type

)

select *
from all_values
where value_field not in (
    'Reactive','Preventive','Installation'
)


