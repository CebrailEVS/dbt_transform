
    
    

with all_values as (

    select
        code_analytique_bu as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
    group by code_analytique_bu

)

select *
from all_values
where value_field not in (
    'COMMERCE','NUNSHEN','NESHU','SUPPORT','TECHNIQUE','PIECES DET','ZSITUATION'
)


