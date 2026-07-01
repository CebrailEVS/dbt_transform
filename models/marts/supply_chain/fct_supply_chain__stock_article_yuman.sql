{{ config(
    materialized='table',
    partition_by={
        'field': 'stock_date',
        'data_type': 'date'
    }
) }}

-- Un emplacement de stock Yuman (nom_du_stock) est soit un DÉPÔT (libellé contenant 'DEPOT',
-- 4 sites), soit un TECHNICIEN (van, actif 'ST - …' ou inactif 'Stock technicien - …').
-- La rupture (quantite = 0) arrive en source sans emplacement (nom_du_stock NULL).

with article_stock as (
    select
        -- Grain
        date(export_date) as stock_date,
        reference,

        -- Attribut metier (min() = choix deterministe : ~181 couples reference/date
        -- portent 2 designations distinctes en source)
        min(designation) as designation,

        -- Disponibilite : global (tous emplacements), depot (reappro), technicien (terrain).
        -- Invariant : is_out_of_stock = is_out_of_stock_depot AND is_out_of_stock_technicien.
        sum(quantite) = 0 as is_out_of_stock,
        sum(case when {{ yuman_is_depot('nom_du_stock') }} then quantite else 0 end) = 0 as is_out_of_stock_depot,
        sum(case when not {{ yuman_is_depot('nom_du_stock') }} then quantite else 0 end) = 0
            as is_out_of_stock_technicien,

        -- Mesures : quantites agregees
        sum(quantite) as total_quantity,
        sum(case when {{ yuman_is_depot('nom_du_stock') }} then quantite else 0 end) as depot_quantity,
        sum(case when not {{ yuman_is_depot('nom_du_stock') }} then quantite else 0 end) as technicien_quantity,

        -- Mesures : nb d'emplacements approvisionnes (quantite > 0)
        count(distinct case
            when {{ yuman_is_depot('nom_du_stock') }} and quantite > 0 then nom_du_stock
        end) as nb_depots_en_stock,
        count(distinct case
            when not {{ yuman_is_depot('nom_du_stock') }} and quantite > 0 then nom_du_stock
        end) as nb_techniciens_en_stock,

        -- Metadonnees dbt
        current_timestamp() as dbt_updated_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from {{ ref('stg_yuman_gcs__stock_theorique') }}
    where reference is not null
    group by stock_date, reference
)

select *
from article_stock
