
    
    

with dbt_test__target as (

  select demand_id as unique_field
  from `evs-datastack-prod`.`prod_marts`.`fct_yuman__workorder_pricing`
  where demand_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


