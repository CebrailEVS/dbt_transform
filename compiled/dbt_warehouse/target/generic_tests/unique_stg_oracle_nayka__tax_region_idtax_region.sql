
    
    

with dbt_test__target as (

  select idtax_region as unique_field
  from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__tax_region`
  where idtax_region is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


