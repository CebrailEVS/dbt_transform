
    
    

with child as (
    select ct_num as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
    where ct_num is not null
),

parent as (
    select ct_num as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptet`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


