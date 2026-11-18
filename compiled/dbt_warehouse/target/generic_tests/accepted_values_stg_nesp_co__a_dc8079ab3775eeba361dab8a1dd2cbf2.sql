
    
    

with all_values as (

    select
        activity_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__activite`
    group by activity_type

)

select *
from all_values
where value_field not in (
    'Phone Call','Appointment','Activity Task','E-Mail'
)


