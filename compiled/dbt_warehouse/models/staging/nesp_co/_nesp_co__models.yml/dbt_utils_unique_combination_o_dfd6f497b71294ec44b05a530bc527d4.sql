





with validation_errors as (

    select
        opportunity_id, employee_responsible, created_by
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__opportunite`
    group by opportunity_id, employee_responsible, created_by
    having count(*) > 1

)

select *
from validation_errors


