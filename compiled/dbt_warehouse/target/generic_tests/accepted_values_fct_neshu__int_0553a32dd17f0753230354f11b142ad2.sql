
    
    

with all_values as (

    select
        demand_status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by demand_status

)

select *
from all_values
where value_field not in (
    'Open','Accepted','Rejected','Closed'
)


