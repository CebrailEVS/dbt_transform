
    
    

with child as (
    select dl_no_in as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
    where dl_no_in is not null
),

parent as (
    select dl_no as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


