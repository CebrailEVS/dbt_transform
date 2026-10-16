
    
    

with all_values as (

    select
        code_status_record as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task`
    group by code_status_record

)

select *
from all_values
where value_field not in (
    -1,0,1
)


