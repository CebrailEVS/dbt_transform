
    
    

with all_values as (

    select
        act_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_commerce__activite`
    group by act_type

)

select *
from all_values
where value_field not in (
    'Rendez-vous','Tâche dactivité','Appel téléphonique','e-mail'
)


