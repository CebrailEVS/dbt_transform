-- models/fct_chargement_quinzaine.sql
{{ config(
    materialized='table',
    description='Table de faits des chargement et consommation (telemetrie) par type de produit, par machine et date de passage appro - Utilisée pour les BI de taux d ecoulement et Suivi des chargements machines gratuités'
) }}

with base as (
    select
        p.product_type,
        cm.company_code,
        extract(year from cm.task_start_date) as annee_chgt,
        floor(
            date_diff(
                date(cm.task_start_date),
                date_trunc(
                    date_trunc(date(cm.task_start_date), year),
                    week (monday)
                ),
                day
            ) / 14
        ) + 1 as quinzaine_chgt,
        cm.load_quantity
    from {{ ref('int_oracle_neshu__chargement_tasks') }} as cm
    left join {{ ref('dim_oracle_neshu__product') }} as p
        on cm.product_id = p.product_id
    where cm.task_start_date >= timestamp(
        datetime_sub(current_datetime(), interval 24 month)
    )
)

select
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt,
    sum(load_quantity) as quantite_chargee,
    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id  -- noqa: TMP
from base
group by
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt
order by
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt
