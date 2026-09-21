
    
    

with dbt_test__target as (

  select ar_ref as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article`
  where ar_ref is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


