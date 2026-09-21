
    
    

with all_values as (

    select
        budg_mois as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__pnl_budget`
    group by budg_mois

)

select *
from all_values
where value_field not in (
    1,2,3,4,5,6,7,8,9,10,11,12
)


