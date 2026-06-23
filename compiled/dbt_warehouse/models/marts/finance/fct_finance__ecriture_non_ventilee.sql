

with __dbt__cte__int_mssql_sage__ecriture_non_ventilee as (


with ventilations as (
    select distinct ec_no
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
),

ecritures_6_7 as (
    select
        ec_no as numero_ecriture_comptable,
        cg_num as numero_compte_general,
        ec_intitule as libelle_ecriture,
        ec_sens as sens_ecriture,
        ec_montant as montant,
        date_add(date(jm_date), interval ec_jour - 1 day) as date_facturation,
        date(ec_date) as date_ecriture_comptable,
        date(jm_date) as date_periode_facturation,
        ec_jour as jour_facturation,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
    where
        (cg_num like '6%' or cg_num like '7%')
        and ec_date >= timestamp('2025-01-01')
)

select e.*
from ecritures_6_7 as e
left join ventilations as v
    on e.numero_ecriture_comptable = v.ec_no
where v.ec_no is null
) select
    date_facturation,
    numero_ecriture_comptable as ecriture_comptable_id,
    numero_compte_general,
    libelle_ecriture,
    sens_ecriture,
    date_ecriture_comptable,
    montant,
    extracted_at
from __dbt__cte__int_mssql_sage__ecriture_non_ventilee