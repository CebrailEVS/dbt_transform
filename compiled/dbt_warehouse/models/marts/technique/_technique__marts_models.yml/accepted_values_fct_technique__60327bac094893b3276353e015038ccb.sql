
    
    

with all_values as (

    select
        billing_validation_status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__workorder_pricing`
    group by billing_validation_status

)

select *
from all_values
where value_field not in (
    'VALIDATED','MISSING_TARIF','UNTRACKABLE','NOT_BILLABLE'
)


