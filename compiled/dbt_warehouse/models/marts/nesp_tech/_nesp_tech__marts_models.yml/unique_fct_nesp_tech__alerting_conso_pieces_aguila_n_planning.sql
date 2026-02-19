
    
    

with dbt_test__target as (

  select n_planning as unique_field
  from `evs-datastack-prod`.`prod_marts`.`fct_nesp_tech__alerting_conso_pieces_aguila`
  where n_planning is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


