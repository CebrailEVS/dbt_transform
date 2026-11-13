
    
    

with all_values as (

    select
        motif_a_categoriser as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`
    group by motif_a_categoriser

)

select *
from all_values
where value_field not in (
    'Sans collaborateur','Collaborateur sans code'
)


