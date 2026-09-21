
    
    

with all_values as (

    select
        delivery_status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__commande_fournisseur_tasks`
    group by delivery_status_code

)

select *
from all_values
where value_field not in (
    'LIVRE','LIVRE_PARTIEL','EN_ATTENTE'
)


