
    
    

with dbt_test__target as (

  select key_ref_inter as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`
  where key_ref_inter is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


