
    
    

with all_values as (

    select
        task_status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__commande_fournisseur_tasks`
    group by task_status_code

)

select *
from all_values
where value_field not in (
    'FAIT','VALIDE','ANNULE','ANOMALIE'
)


