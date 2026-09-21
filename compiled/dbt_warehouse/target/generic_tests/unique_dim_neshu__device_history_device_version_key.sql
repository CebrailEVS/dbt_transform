
    
    

with dbt_test__target as (

  select device_version_key as unique_field
  from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device_history`
  where device_version_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


