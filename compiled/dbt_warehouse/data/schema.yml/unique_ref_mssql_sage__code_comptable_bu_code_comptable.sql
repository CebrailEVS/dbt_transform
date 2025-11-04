
    
    

with dbt_test__target as (

  select code_comptable as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__code_comptable_bu`
  where code_comptable is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


