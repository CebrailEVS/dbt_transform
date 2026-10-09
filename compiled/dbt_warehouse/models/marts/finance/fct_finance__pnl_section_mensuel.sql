

with ecritures as (
    select
        date(date_trunc(date_facturation, month)) as mois_date,
        code_analytique,
        coalesce(code_analytique_bu, 'BU_NON_RENSEIGNEE') as code_analytique_bu,
        numero_compte_general,
        categorie_pnl_bu,
        macro_categorie_pnl_bu,
        montant_analytique_signe
    from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
    -- Sans ventilation analytique, pas de section : ces écritures restent dans fct_finance__pnl_bu.
    -- Plan 1 explicite : un 2e plan analytique Sage doublerait les montants sans casser le grain.
    where
        not is_missing_analytical
        and numero_plan_analytique = 1
),

sections as (
    select
        ca_num as code_analytique,
        ca_intitule as section_analytique_intitule
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptea`
    where n_analytique = 1
)

select
    e.mois_date,
    e.code_analytique,
    e.code_analytique_bu,
    e.numero_compte_general,
    s.section_analytique_intitule,
    e.categorie_pnl_bu,
    e.macro_categorie_pnl_bu,
    sum(e.montant_analytique_signe) as montant_signe
from ecritures as e
left join sections as s
    on e.code_analytique = s.code_analytique
group by
    e.mois_date,
    e.code_analytique,
    e.code_analytique_bu,
    e.numero_compte_general,
    s.section_analytique_intitule,
    e.categorie_pnl_bu,
    e.macro_categorie_pnl_bu