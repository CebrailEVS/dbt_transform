
    
    

with all_values as (

    select
        forced_source as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__consommation_source_override`
    group by forced_source

)

select *
from all_values
where value_field not in (
    'TELEMETRIE','CHARGEMENT'
)


