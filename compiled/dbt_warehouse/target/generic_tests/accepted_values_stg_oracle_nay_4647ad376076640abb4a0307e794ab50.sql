
    
    

with all_values as (

    select
        type_product_source as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task`
    group by type_product_source

)

select *
from all_values
where value_field not in (
    'COMPANY','DEVICE'
)


