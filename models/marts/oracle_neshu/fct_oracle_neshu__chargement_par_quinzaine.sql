-- models/fct_chargement_quinzaine.sql
{{ config(
    materialized='table',
    description='Table de faits des chargement et consommation (telemetrie) par type de produit, par machine et date de passage appro - Utilisée pour les BI de taux d ecoulement et Suivi des chargements machines gratuités'
) }}

with base as (
    select
        p.product_type,
        cm.company_code,
        comp.name as company_name,
        EXTRACT(year from cm.task_start_date) as annee_chgt,
        FLOOR(
            DATE_DIFF(
                DATE(cm.task_start_date),
                DATE_TRUNC(
                    DATE_TRUNC(DATE(cm.task_start_date), year),
                    week (monday)
                ),
                day
            ) / 14
        ) + 1 as quinzaine_chgt,
        cm.load_quantity
    from {{ ref('int_oracle_neshu__chargement_tasks') }} as cm
    left join {{ ref('dim_oracle_neshu__product') }} as p
        on cm.product_id = p.product_id
    inner join {{ ref('dim_oracle_neshu__device') }} as d
        on
            cm.device_id = d.device_id
            and d.device_economic_model = 'Gratuit'
    left join {{ ref('stg_oracle_neshu__company') }} as comp
        on cm.company_id = comp.idcompany
    where cm.task_start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), interval 730 day)
)

select
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt,
    SUM(load_quantity) as quantite_chargee,
    -- Métadonnées dbt
    CURRENT_TIMESTAMP() as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    company_name,
    annee_chgt,
    quinzaine_chgt
