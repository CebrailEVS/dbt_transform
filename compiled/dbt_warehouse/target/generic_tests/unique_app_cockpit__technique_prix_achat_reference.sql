
    
    

with dbt_test__target as (

  select reference as unique_field
  from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_prix_achat`
  where reference is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


