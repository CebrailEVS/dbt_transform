{{ config(
    materialized='table',
    partition_by={
        'field': 'stock_date',
        'data_type': 'date'
    },
    description='Table de faits des stocks des pièces Yuman journaliers pour chaque stock technicien et dépôt'
) }}

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
        '{{ invocation_id }}' as dbt_invocation_id

    from {{ ref('stg_yuman_gcs__stock_theorique') }}
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks
