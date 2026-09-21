
    
    

with dbt_test__target as (

  select co_secteur as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_nesp_co__commerciaux`
  where co_secteur is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


