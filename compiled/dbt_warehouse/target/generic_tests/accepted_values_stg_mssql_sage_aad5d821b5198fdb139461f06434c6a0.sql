
    
    

with all_values as (

    select
        do_domaine as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docentete`
    group by do_domaine

)

select *
from all_values
where value_field not in (
    0,1,2
)


