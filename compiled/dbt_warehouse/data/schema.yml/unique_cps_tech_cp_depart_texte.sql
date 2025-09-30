
    
    

with dbt_test__target as (

  select cp_depart_texte as unique_field
  from `evs-datastack-prod`.`prod_reference`.`cps_tech`
  where cp_depart_texte is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


