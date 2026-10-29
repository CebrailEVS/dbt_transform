
    
    

with all_values as (

    select
        resource_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__node_result`
    group by resource_type

)

select *
from all_values
where value_field not in (
    'model','test','unit_test','seed','snapshot'
)


