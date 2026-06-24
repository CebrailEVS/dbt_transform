

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
        '4f44ca2a-7a87-45ac-b0fb-0103efdd5d3f' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks