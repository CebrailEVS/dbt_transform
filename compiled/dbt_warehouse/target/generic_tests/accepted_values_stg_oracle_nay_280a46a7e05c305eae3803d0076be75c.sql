
    
    

with all_values as (

    select
        code_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__resources_type`
    group by code_type

)

select *
from all_values
where value_field not in (
    'SYSTEM','PERSON','VEHICLE','SUBCONTRACTOR','ROOM','ENDOWMENT','ETAG'
)


