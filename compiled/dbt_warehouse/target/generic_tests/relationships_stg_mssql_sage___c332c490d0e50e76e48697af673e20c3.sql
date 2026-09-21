
    
    

with child as (
    select cg_num as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
    where cg_num is not null
),

parent as (
    select cg_num as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_compteg`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


