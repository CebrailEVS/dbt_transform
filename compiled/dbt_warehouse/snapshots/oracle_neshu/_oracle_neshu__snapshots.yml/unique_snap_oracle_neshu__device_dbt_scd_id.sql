
    
    

with dbt_test__target as (

  select dbt_scd_id as unique_field
  from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__device`
  where dbt_scd_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


