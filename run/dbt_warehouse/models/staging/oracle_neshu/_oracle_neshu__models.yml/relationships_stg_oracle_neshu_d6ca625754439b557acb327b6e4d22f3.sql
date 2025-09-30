
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select idcompany_owner as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device`
    where idcompany_owner is not null
),

parent as (
    select idcompany as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test