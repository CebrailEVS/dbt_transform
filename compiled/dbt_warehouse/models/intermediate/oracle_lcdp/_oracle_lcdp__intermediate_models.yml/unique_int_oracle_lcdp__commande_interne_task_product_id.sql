
    
    

with dbt_test__target as (

  select task_product_id as unique_field
  from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__commande_interne`
  where task_product_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


