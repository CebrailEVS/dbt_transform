-- models/fct_chargement_quinzaine.sql
{{ config(
    materialized='table',
    description='Table de faits des chargement et consommation (telemetrie) par type de produit, par machine et date de passage appro - Utilisée pour les BI de taux d ecoulement et Suivi des chargements machines gratuités'
) }}

WITH base AS (
    SELECT
        p.product_type,
        cm.company_code,
        EXTRACT(YEAR FROM cm.task_start_date) AS annee_chgt,
        FLOOR(
            DATE_DIFF(
                DATE(cm.task_start_date),
                DATE_TRUNC(
                    DATE_TRUNC(DATE(cm.task_start_date), YEAR),
                    WEEK(MONDAY)
                ),
                DAY
            ) / 14
        ) + 1 AS quinzaine_chgt,
        cm.load_quantity
    FROM {{ ref('int_oracle_neshu__chargement_tasks') }} cm
    LEFT JOIN {{ ref('dim_oracle_neshu__product') }} p
        ON cm.product_id = p.product_id
    WHERE cm.task_start_date >= TIMESTAMP(
        DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 24 MONTH)
    )
)
SELECT
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt,
    SUM(load_quantity) AS quantite_chargee,
    -- Métadonnées dbt
    CURRENT_TIMESTAMP() as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id
FROM base
GROUP BY
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt
ORDER BY
    product_type,
    company_code,
    annee_chgt,
    quinzaine_chgt