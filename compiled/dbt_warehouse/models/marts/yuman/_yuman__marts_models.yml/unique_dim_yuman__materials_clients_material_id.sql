
    
    

with dbt_test__target as (

  select material_id as unique_field
  from `evs-datastack-prod`.`prod_marts`.`dim_yuman__materials_clients`
  where material_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


