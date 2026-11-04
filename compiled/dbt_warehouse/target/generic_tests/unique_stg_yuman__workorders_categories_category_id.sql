
    
    

with dbt_test__target as (

  select category_id as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders_categories`
  where category_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


