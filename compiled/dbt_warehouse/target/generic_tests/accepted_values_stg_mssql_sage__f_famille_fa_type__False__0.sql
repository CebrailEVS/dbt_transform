
    
    

with all_values as (

    select
        fa_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_famille`
    group by fa_type

)

select *
from all_values
where value_field not in (
    0
)


