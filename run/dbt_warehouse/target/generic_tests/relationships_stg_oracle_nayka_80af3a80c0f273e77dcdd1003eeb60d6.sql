
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select idtax_region as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_amount`
    where idtax_region is not null
),

parent as (
    select idtax_region as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__tax_region`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test