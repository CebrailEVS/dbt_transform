
    
    

with all_values as (

    select
        kpi as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_bu`
    group by kpi

)

select *
from all_values
where value_field not in (
    'CA','CONSOMMATION_MP_SSTT','MASSE_SALARIALE','FRAIS_DIRECTS_AMORTISSEMENTS','MARGE_BRUTE','MARGE_NETTE'
)


