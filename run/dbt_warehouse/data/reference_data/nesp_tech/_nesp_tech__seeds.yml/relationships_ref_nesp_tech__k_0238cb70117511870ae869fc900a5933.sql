
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select machine_clean as from_field
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`
    where machine_clean is not null
),

parent as (
    select machine_clean as to_field
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test