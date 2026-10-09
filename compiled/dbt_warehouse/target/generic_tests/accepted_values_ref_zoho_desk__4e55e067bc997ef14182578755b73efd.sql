
    
    

with all_values as (

    select
        metric as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__sla_buckets`
    group by metric

)

select *
from all_values
where value_field not in (
    'first_response','resolution'
)


