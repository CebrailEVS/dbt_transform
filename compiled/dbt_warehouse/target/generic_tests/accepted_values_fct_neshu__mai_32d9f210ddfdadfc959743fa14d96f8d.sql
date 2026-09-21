
    
    

with all_values as (

    select
        source_last_preventive as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__maintenance_preventive`
    group by source_last_preventive

)

select *
from all_values
where value_field not in (
    'yuman','dlog','aucune'
)


