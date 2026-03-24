
    
    

with dbt_test__target as (

  select key_inter as unique_field
  from `evs-datastack-prod`.`prod_marts`.`fct_technique__interventions`
  where key_inter is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


