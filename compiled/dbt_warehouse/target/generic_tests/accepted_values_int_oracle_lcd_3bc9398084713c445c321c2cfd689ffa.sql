
    
    

with all_values as (

    select
        load_type_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__chargement_tasks`
    group by load_type_code

)

select *
from all_values
where value_field not in (
    'LOADING','REMOVING'
)


