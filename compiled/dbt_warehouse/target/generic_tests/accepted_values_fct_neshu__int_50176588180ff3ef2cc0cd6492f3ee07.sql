
    
    

with all_values as (

    select
        statut_inter as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by statut_inter

)

select *
from all_values
where value_field not in (
    'TERMINATED','TOTECH'
)


