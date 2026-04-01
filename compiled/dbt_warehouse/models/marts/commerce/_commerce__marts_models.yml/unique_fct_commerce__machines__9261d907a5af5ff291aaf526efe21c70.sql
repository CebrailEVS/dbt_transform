
    
    

with dbt_test__target as (

  select n_serie_machine as unique_field
  from `evs-datastack-prod`.`prod_marts`.`fct_commerce__machines_avec_interventions`
  where n_serie_machine is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


