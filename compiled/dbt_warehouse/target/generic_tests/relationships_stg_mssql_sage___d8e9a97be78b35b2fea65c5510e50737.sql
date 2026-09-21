
    
    

with child as (
    select ec_no as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
    where ec_no is not null
),

parent as (
    select ec_no as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


