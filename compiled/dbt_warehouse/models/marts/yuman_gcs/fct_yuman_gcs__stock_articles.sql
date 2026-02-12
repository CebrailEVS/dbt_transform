

with filtered_stocks as (
    select
        -- Attributs metier
        reference,
        designation,
        nom_du_stock as stock,

        -- Mesure
        quantite,

        -- Date
        date(export_date) as stock_date,

        -- Metadonnees dbt
        current_timestamp() as dbt_updated_at,
        'e9bb3e43-2aaf-43b4-8dab-81cc8df62050' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks