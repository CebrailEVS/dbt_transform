
    
    

with child as (
    select cbco_no as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
    where cbco_no is not null
),

parent as (
    select co_no as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_collaborateur`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


