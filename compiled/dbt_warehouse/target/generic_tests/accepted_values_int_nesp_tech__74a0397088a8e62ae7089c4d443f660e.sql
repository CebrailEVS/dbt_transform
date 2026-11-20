
    
    

with all_values as (

    select
        statut_facturation as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__intervention_technique`
    group by statut_facturation

)

select *
from all_values
where value_field not in (
    'VALIDATED','NOT VALIDATED','NOT DEFINED'
)


