





with validation_errors as (

    select
        metric, bucket_order
    from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__sla_buckets`
    group by metric, bucket_order
    having count(*) > 1

)

select *
from validation_errors


