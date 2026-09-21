
    
    

with all_values as (

    select
        typologie as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__pnl_client_mensuel`
    group by typologie

)

select *
from all_values
where value_field not in (
    'GET','OTHER'
)


