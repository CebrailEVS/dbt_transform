
    
    

with all_values as (

    select
        classe_machine as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__classe_modele_machine`
    group by classe_machine

)

select *
from all_values
where value_field not in (
    'Machine principale','Machine (comptoir ou à monter)','Module / composant'
)


