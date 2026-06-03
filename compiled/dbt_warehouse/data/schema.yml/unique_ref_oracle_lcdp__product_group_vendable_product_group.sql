
    
    

with dbt_test__target as (

  select product_group as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_oracle_lcdp__product_group_vendable`
  where product_group is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


