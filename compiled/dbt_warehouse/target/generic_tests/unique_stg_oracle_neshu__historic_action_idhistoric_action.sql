
    
    

with dbt_test__target as (

  select idhistoric_action as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__historic_action`
  where idhistoric_action is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


