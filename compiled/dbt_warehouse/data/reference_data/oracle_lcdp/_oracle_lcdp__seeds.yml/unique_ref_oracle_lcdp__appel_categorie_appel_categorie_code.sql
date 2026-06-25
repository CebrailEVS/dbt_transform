
    
    

with dbt_test__target as (

  select appel_categorie_code as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_oracle_lcdp__appel_categorie`
  where appel_categorie_code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


