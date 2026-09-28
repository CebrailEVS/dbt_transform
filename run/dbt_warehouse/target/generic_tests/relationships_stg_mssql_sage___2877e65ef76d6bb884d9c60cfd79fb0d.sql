
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select do_tiers as from_field
    from (select * from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docentete` where do_domaine in (0, 1)) dbt_subquery
    where do_tiers is not null
),

parent as (
    select ct_num as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptet`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test