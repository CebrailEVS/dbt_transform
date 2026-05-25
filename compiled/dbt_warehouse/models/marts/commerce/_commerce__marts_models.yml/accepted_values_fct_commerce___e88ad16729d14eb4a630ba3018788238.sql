
    
    

with all_values as (

    select
        opp_statut as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_commerce__opportunite`
    group by opp_statut

)

select *
from all_values
where value_field not in (
    'Gagné','Perdu','En cours'
)


