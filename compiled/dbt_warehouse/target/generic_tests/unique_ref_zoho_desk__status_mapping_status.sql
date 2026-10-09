
    
    

with dbt_test__target as (

  select status as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__status_mapping`
  where status is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


