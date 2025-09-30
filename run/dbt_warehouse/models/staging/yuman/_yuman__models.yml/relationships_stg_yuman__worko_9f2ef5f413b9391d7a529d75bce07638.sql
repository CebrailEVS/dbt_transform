
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select site_id as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders`
    where site_id is not null
),

parent as (
    select site_id as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test