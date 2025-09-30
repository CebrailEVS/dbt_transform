
    
    

with dbt_test__target as (

  select id as unique_field
  from `evs-datastack-prod`.`prod_raw`.`yuman_purchase_orders`
  where id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


