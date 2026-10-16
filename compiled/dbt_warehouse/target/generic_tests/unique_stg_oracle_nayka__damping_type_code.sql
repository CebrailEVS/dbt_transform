
    
    

with dbt_test__target as (

  select code as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__damping_type`
  where code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


