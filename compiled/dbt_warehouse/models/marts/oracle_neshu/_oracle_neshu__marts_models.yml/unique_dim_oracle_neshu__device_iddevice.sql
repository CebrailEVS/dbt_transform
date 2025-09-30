
    
    

with dbt_test__target as (

  select iddevice as unique_field
  from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device`
  where iddevice is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


