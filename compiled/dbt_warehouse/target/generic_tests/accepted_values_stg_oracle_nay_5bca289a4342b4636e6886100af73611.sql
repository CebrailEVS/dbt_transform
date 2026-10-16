
    
    

with all_values as (

    select
        real_planned as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_product`
    group by real_planned

)

select *
from all_values
where value_field not in (
    'R','P'
)


