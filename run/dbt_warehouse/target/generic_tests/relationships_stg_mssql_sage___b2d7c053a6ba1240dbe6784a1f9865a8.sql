
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select dl_no_out as from_field
    from (select * from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie` where dl_no_out != 0) dbt_subquery
    where dl_no_out is not null
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



  
  
      
    ) dbt_internal_test