





with validation_errors as (

    select
        ticket_id, segment_idx
    from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_lifecycle_segments`
    group by ticket_id, segment_idx
    having count(*) > 1

)

select *
from validation_errors


