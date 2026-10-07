
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select ar_ref as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
    where ar_ref is not null
),

parent as (
    select ar_ref as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test