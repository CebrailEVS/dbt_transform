
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select report_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_bi__usage_rapport`
    where report_id is not null
),

parent as (
    select report_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test