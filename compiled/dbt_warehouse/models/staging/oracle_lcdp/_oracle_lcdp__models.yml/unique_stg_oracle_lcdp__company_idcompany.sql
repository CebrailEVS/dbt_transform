
    
    

with dbt_test__target as (

  select idcompany as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company`
  where idcompany is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


