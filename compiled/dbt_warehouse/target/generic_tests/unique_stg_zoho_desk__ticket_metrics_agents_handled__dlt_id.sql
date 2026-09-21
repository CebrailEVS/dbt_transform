
    
    

with dbt_test__target as (

  select _dlt_id as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_metrics_agents_handled`
  where _dlt_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


