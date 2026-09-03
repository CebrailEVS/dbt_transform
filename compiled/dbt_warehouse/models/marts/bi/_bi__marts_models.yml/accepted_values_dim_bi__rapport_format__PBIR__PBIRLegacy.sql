
    
    

with all_values as (

    select
        format as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
    group by format

)

select *
from all_values
where value_field not in (
    'PBIR','PBIRLegacy'
)


