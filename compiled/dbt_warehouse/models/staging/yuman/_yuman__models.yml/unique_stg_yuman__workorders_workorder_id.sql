
    
    

with dbt_test__target as (

  select workorder_id as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders`
  where workorder_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


