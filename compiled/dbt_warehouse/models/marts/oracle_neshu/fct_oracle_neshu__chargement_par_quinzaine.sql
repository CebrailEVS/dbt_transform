-- models/fct_chargement_quinzaine.sql


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
    FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` cm
    LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` p
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
    '6ffad0ad-5a06-4ebd-b471-faad483e9754' as dbt_invocation_id
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