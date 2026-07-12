
    
    

with all_values as (

    select
        saison as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_general__calendrier_saison`
    group by saison

)

select *
from all_values
where value_field not in (
    'ete','hiver'
)


