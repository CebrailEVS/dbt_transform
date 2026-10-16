
    
    

with dbt_test__target as (

  select document_number as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task`
  where document_number is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


