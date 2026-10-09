
    
    

with all_values as (

    select
        status_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__status_mapping`
    group by status_type

)

select *
from all_values
where value_field not in (
    'Open','On Hold','Closed'
)


