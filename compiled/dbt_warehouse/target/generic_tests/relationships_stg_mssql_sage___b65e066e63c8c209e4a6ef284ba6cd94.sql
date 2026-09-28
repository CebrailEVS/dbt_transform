
    
    

with child as (
    select fa_code_famille as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article`
    where fa_code_famille is not null
),

parent as (
    select fa_code_famille as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_famille`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


