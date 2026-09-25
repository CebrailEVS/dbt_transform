
    
    

with dbt_test__target as (

  select appel_numero as unique_field
  from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__appel_sav`
  where appel_numero is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


