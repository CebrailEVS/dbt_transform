{{ config(materialized='ephemeral') }}

with ventilations as (
    select distinct ec_no
    from {{ ref('stg_mssql_sage__f_ecriturea') }}
),

ecritures_6_7 as (
    select
        ec_no as numero_ecriture_comptable,
        cg_num as numero_compte_general,
        ec_intitule as libelle_ecriture,
        ec_sens as sens_ecriture,
        ec_montant as montant,
        case when ec_sens = 1 then abs(ec_montant) else -abs(ec_montant) end as montant_signe,
        date_add(date(jm_date), interval ec_jour - 1 day) as date_facturation,
        date(ec_date) as date_ecriture_comptable,
        date(jm_date) as date_periode_facturation,
        ec_jour as jour_facturation,
        extracted_at
    from {{ ref('stg_mssql_sage__f_ecriturec') }}
    where
        (cg_num like '6%' or cg_num like '7%')
        and ec_date >= timestamp('{{ var("ecriture_non_ventilee_floor") }}')
)

select e.*
from ecritures_6_7 as e
left join ventilations as v
    on e.numero_ecriture_comptable = v.ec_no
where v.ec_no is null
