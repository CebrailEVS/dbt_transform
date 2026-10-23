
    
    

with all_values as (

    select
        affichage as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_technique_tasks`
    group by affichage

)

select *
from all_values
where value_field not in (
    'PASAFFICHAGE','OUPS'
)


