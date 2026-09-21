
    
    

with all_values as (

    select
        rupture_statut as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
    group by rupture_statut

)

select *
from all_values
where value_field not in (
    'RUPTURE_TOTALE','STOCK_RESTANT_VANS','STOCK_AILLEURS'
)


