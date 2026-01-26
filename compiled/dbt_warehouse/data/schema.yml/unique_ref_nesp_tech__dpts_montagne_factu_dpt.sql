
    
    

with dbt_test__target as (

  select dpt as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__dpts_montagne_factu`
  where dpt is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


