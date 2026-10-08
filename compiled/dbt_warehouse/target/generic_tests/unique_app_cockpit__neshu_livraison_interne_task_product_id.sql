
    
    

with dbt_test__target as (

  select task_product_id as unique_field
  from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_livraison_interne`
  where task_product_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


