
    
    

with dbt_test__target as (

  select reference_gac as unique_field
  from `evs-datastack-prod`.`prod_marts`.`fct_services_generaux__sinistre`
  where reference_gac is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


