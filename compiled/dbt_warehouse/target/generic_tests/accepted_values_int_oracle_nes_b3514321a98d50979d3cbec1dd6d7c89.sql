
    
    

with all_values as (

    select
        ca_category as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__facturation_tasks`
    group by ca_category

)

select *
from all_values
where value_field not in (
    'VENDING','PRESTA_SERVICE','FONTAINES','LAVES_VERRES','NEGOCE'
)


