
    
    

with all_values as (

    select
        evt_event_unit as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_events`
    group by evt_event_unit

)

select *
from all_values
where value_field not in (
    'Journee','Heure'
)


