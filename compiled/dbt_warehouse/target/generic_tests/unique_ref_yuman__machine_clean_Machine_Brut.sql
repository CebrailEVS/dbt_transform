
    
    

with dbt_test__target as (

  select Machine_Brut as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_yuman__machine_clean`
  where Machine_Brut is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


