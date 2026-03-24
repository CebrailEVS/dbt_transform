
    
    

with all_values as (

    select
        src_inter as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_tech_evs__interventions_enriched`
    group by src_inter

)

select *
from all_values
where value_field not in (
    'NESP','YUMAN'
)


