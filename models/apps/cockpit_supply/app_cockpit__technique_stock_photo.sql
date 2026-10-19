{{ config(
    materialized='table',
    partition_by={
        'field': 'stock_date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['stock', 'reference']
) }}

-- Photos de stock TechCare dont l'app a besoin : la première photo GLOBALE de chaque
-- mois (stock d'ouverture / de clôture des flux) et la dernière photo de chaque ZONE
-- (dépôt ou van) dans chaque mois (stock courant, fin de mois des dépôts). Par zone et
-- non par (zone × article) : chez Yuman une ligne absente vaut stock 0, une référence
-- sortie d'une zone en cours de mois doit donc disparaître de sa photo de fin de mois.
-- Toutes les lignes de ces jours sont gardées, doublons compris (l'app les additionne).
with stock_yuman as (
    -- Stock théorique Yuman exporté chaque jour : une ligne par zone (dépôt ou van) et référence.
    select
        reference,
        designation,
        nom_du_stock as stock,
        {{ yuman_stock_type('nom_du_stock') }} as type_stock,
        quantite,
        date(export_date) as stock_date
    from {{ ref('stg_yuman_evs_sftp__stock_theorique') }}
    where
        reference is not null
        and nom_du_stock is not null
),

photos as (
    select
        *,
        min(stock_date) over (partition by date_trunc(stock_date, month)) as d_premiere_mois,
        max(stock_date) over (partition by stock, date_trunc(stock_date, month)) as d_derniere_zone_mois,
        max(stock_date) over () as d_courante
    from stock_yuman
)

select
    stock_date,
    stock,
    type_stock,
    reference,
    designation,
    quantite,
    stock_date = d_premiere_mois as is_premiere_photo_mois,
    stock_date = d_derniere_zone_mois as is_derniere_photo_zone_mois,
    stock_date = d_courante as is_photo_courante
from photos
where stock_date = d_premiere_mois or stock_date = d_derniere_zone_mois
