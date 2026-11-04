
    
    

with all_values as (

    select
        ct_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptet`
    group by ct_type

)

select *
from all_values
where value_field not in (
    0,1,2,3
)


