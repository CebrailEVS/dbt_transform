

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
        '01a0d8df-9637-70b1-9631-fa5b9d2b17e7' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks