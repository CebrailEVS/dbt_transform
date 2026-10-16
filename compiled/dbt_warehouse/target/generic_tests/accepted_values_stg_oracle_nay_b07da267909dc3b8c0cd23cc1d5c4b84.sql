
    
    

with all_values as (

    select
        code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__damping_type`
    group by code

)

select *
from all_values
where value_field not in (
    'LINEAR','DECREASING','LEASE-PURCHASE'
)


