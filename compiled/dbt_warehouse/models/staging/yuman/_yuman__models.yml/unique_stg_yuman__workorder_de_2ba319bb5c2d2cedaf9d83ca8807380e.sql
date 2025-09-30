
    
    

with dbt_test__target as (

  select demand_category_id as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands_categories`
  where demand_category_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


