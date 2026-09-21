
    
    

with dbt_test__target as (

  select idproduct_unit as unique_field
  from `evs-datastack-prod`.`prod_raw`.`evs_product_unit`
  where idproduct_unit is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


