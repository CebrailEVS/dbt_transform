
    
    

with all_values as (

    select
        calendrier as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__general_calendrier_ouvre`
    group by calendrier

)

select *
from all_values
where value_field not in (
    'METROPOLE','ALSACE_MOSELLE'
)


