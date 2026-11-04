
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select _zoho_desk_associated_tickets_id as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history`
    where _zoho_desk_associated_tickets_id is not null
),

parent as (
    select ticket_id as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__tickets`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test