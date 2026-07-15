
    
    

with all_values as (

    select
        a_facturer as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_mee`
    group by a_facturer

)

select *
from all_values
where value_field not in (
    'OUI','NON'
)


