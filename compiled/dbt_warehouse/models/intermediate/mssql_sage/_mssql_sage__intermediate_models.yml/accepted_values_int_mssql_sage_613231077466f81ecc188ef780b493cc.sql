
    
    

with all_values as (

    select
        macro_categorie_pnl_bu as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
    group by macro_categorie_pnl_bu

)

select *
from all_values
where value_field not in (
    'Chiffre d\'Affaires','Masse Salariale','Frais Directs & Amortissements','Consommation MP & SSTT'
)


