
    
    

with dbt_test__target as (

  select dl_no as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
  where dl_no is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


