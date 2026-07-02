

with filtered_stocks as (
    select
        -- Attributs metier
        reference,
        designation,
        nom_du_stock as stock,
        case when (nom_du_stock like '%DEPOT%') then 'DEPOT' else 'TECHNICIEN' end as type_stock,

        -- Mesure
        quantite,

        -- Date
        date(export_date) as stock_date,

        -- Metadonnees dbt
        current_timestamp() as dbt_updated_at,
        '32ab3e31-43be-4871-883e-50e103b89190' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks