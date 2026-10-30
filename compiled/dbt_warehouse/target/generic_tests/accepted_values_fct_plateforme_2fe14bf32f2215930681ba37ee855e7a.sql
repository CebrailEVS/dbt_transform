
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_plateforme__resultat_dbt`
    group by status

)

select *
from all_values
where value_field not in (
    'success','error','skipped','pass','warn','fail'
)


