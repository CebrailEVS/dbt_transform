
    
    

with all_values as (

    select
        src_inter as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__interventions`
    group by src_inter

)

select *
from all_values
where value_field not in (
    'NESP','YUMAN'
)


