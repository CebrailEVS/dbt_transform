
    
    

with all_values as (

    select
        type_stock as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
    group by type_stock

)

select *
from all_values
where value_field not in (
    'DEPOT','TECHNICIEN'
)


