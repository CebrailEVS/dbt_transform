
    
    

with dbt_test__target as (

  select idlabel_family as unique_field
  from `evs-datastack-prod`.`prod_raw`.`lcdp_label_family`
  where idlabel_family is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


