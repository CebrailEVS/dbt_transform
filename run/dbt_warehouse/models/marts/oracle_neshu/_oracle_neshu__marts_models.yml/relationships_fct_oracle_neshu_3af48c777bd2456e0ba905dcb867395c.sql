
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select company_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`
    where company_id is not null
),

parent as (
    select company_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test