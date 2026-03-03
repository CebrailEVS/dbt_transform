
    
    

with dbt_test__target as (

  select third as unique_field
  from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__clients_enrichis`
  where third is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


