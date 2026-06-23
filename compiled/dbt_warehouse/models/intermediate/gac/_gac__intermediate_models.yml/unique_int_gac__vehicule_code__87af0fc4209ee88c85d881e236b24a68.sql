
    
    

with dbt_test__target as (

  select contrat_immatriculation_edi as unique_field
  from `evs-datastack-prod`.`prod_intermediate`.`int_gac__vehicule_code_analytique`
  where contrat_immatriculation_edi is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


