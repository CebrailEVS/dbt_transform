
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select idtask as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_resources`
    where idtask is not null
),

parent as (
    select idtask as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test