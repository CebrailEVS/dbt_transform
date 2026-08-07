
    
    

with all_values as (

    select
        agency as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
    group by agency

)

select *
from all_values
where value_field not in (
    'evs','evs idf','evs paris','evs paris 2'
)


