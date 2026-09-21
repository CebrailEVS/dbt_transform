
    
    

with all_values as (

    select
        etat_intervention as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_commerce__machine_intervention`
    group by etat_intervention

)

select *
from all_values
where value_field not in (
    'terminée signée','signature différée','terminée non signée'
)


