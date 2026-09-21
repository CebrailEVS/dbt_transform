
    
    

with dbt_test__target as (

  select idtask_has_product as unique_field
  from `evs-datastack-prod`.`prod_raw`.`lcdp_task_has_product`
  where idtask_has_product is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


