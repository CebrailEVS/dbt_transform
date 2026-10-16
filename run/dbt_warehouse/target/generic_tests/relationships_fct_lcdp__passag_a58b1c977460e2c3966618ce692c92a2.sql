
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select resources_roadman_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__passage_appro`
    where resources_roadman_id is not null
),

parent as (
    select resources_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test