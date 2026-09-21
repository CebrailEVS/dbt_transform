
    
    

with all_values as (

    select
        priority as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_enriched`
    group by priority

)

select *
from all_values
where value_field not in (
    'Low','Medium','High','Urgent'
)


