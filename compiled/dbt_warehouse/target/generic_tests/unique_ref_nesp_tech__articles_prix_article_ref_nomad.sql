
    
    

with dbt_test__target as (

  select article_ref_nomad as unique_field
  from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix`
  where article_ref_nomad is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


