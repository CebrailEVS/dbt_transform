
    
    

with all_values as (

    select
        co_fonction as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_co__commerciaux`
    group by co_fonction

)

select *
from all_values
where value_field not in (
    'ALL','alternant rs','commercial','commercial','fidelisation','manager'
)


