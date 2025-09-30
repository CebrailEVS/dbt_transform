
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select idproduct_type as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product`
    where idproduct_type is not null
),

parent as (
    select idproduct_type as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product_type`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test