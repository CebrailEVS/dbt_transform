
    
    

with dbt_test__target as (

  select code_comptable as unique_field
  from `evs-datastack-prod`.`prod_reference`.`mapping_code_comptable__bu`
  where code_comptable is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


