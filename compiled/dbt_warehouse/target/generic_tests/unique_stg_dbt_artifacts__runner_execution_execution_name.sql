
    
    

with dbt_test__target as (

  select execution_name as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
  where execution_name is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


