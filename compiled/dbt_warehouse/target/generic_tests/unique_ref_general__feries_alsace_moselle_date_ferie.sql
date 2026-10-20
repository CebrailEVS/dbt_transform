
    
    

with dbt_test__target as (

  select date_ferie as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_alsace_moselle`
  where date_ferie is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


