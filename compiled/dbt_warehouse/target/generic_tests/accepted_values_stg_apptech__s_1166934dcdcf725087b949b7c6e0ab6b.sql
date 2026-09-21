
    
    

with all_values as (

    select
        doubler_prime as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_pause`
    group by doubler_prime

)

select *
from all_values
where value_field not in (
    'OUI','NON'
)


