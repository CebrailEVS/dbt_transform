
    
    

with child as (
    select de_no as from_field
    from (select * from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` where de_no != 0) dbt_subquery
    where de_no is not null
),

parent as (
    select de_no as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


