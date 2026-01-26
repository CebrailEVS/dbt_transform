
    
    

with dbt_test__target as (

  select nom_machine as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
  where nom_machine is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


