





with validation_errors as (

    select
        budg_bu, budg_categorie_pnl, budg_annee, budg_mois
    from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__pnl_budget`
    group by budg_bu, budg_categorie_pnl, budg_annee, budg_mois
    having count(*) > 1

)

select *
from validation_errors


