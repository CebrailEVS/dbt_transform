
    
    

with all_values as (

    select
        workorder_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by workorder_type

)

select *
from all_values
where value_field not in (
    'Reactive','Preventive','Installation'
)


