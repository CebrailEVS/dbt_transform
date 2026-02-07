
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select idcompany as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_company`
    where idcompany is not null
),

parent as (
    select idcompany as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test