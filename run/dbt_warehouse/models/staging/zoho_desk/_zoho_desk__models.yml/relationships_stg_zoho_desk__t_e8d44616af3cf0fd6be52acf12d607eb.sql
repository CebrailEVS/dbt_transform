
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select contact_id as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__tickets`
    where contact_id is not null
),

parent as (
    select contact_id as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__contacts`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test