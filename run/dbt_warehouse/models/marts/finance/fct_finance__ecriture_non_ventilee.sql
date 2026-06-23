

  create or replace view `evs-datastack-prod`.`prod_marts`.`fct_finance__ecriture_non_ventilee`
  OPTIONS(
      description="""[QUOI M\u00c9TIER] Liste des \u00e9critures comptables de charge (classe 6) ou produit (classe 7) sans aucune ventilation analytique dans Sage \u2014 la file des \u00e9critures restant \u00e0 ventiler par la comptabilit\u00e9, consomm\u00e9e par l'app compta.\n[COMMENT CONSTRUITE] int_mssql_sage__ecriture_non_ventilee : f_ecriturec filtr\u00e9 aux classes 6/7 et >= var('ecriture_non_ventilee_floor', d\u00e9faut 2025-01-01), en anti-jointure sur les \u00e9critures analytiques f_ecriturea (ec_no absents). date_facturation = jm_date + (ec_jour-1) jours.\n[GRAIN] 1 ligne par \u00e9criture comptable (ecriture_comptable_id).\n[NOTES] Mat\u00e9rialis\u00e9e en **view** (et non table) : d\u00e9viation assum\u00e9e \u00e0 la convention marts, justifi\u00e9e par un petit volume (~27k lignes), une conso op\u00e9rationnelle par l'app compta (/mnt/data/project/compta) sensible \u00e0 la fra\u00eecheur et un requ\u00eatage peu fr\u00e9quent. montant brut non sign\u00e9 ; sens_ecriture (0 = d\u00e9bit, 1 = cr\u00e9dit) pour l'interpr\u00e9ter.\n"""
    )
  as 

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
        case when ec_sens = 1 then abs(ec_montant) else -abs(ec_montant) end as montant_signe,
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
    montant_signe,
    extracted_at
from __dbt__cte__int_mssql_sage__ecriture_non_ventilee;

