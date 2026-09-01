
    
    

with all_values as (

    select
        statut_facturation as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention`
    group by statut_facturation

)

select *
from all_values
where value_field not in (
    'VALIDATED','NOT VALIDATED','NOT DEFINED','UNTRACKABLE','MISSING_TARIF','NOT_BILLABLE'
)


