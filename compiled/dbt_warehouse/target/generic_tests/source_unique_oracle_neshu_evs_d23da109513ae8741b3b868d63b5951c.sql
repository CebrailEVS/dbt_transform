
    
    

with dbt_test__target as (

  select idtreatment_by_batch as unique_field
  from `evs-datastack-prod`.`prod_raw`.`evs_treatment_by_batch`
  where idtreatment_by_batch is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


