{{ config(
    materialized='table',
    partition_by={'field': 'stock_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['depot_id', 'reference']
) }}

-- Stock Nunshen par dépôt et référence (Sage, photos quotidiennes de f_artstock) pour
-- Cockpit Supply : remplace les imports « Stock Wissous » (quantités) et « Valo stock »
-- (valeur, CMUP). Une photo par jour (la dernière extraction du jour, heure de Paris) ;
-- gardées : la photo courante et la dernière photo de chaque mois (flux et CODIR).
-- Stock disponible de l'app = dépôt NUNSHEN (depot_id 1) ; valeur de stock = tous dépôts.
with extractions as (
    select distinct extracted_at
    from {{ ref('stg_mssql_sage__f_artstock') }}
),
photos as (
    select
        extracted_at,
        date(extracted_at, 'Europe/Paris') as stock_date
    from extractions
    qualify row_number() over (partition by date(extracted_at, 'Europe/Paris') order by extracted_at desc) = 1
),
reperes as (
    select
        *,
        stock_date = max(stock_date) over (partition by date_trunc(stock_date, month)) as is_derniere_photo_mois,
        stock_date = max(stock_date) over () as is_photo_courante
    from photos
)
select
    r.stock_date,
    st.de_no as depot_id,
    dep.de_intitule as depot,
    st.ar_ref as reference,
    ar.ar_design as designation,
    ar.fa_code_famille as code_famille,
    cast(st.as_qte_sto as float64) as stock_reel,
    cast(st.as_mont_sto as float64) as valorisation,
    cast(safe_divide(st.as_mont_sto, st.as_qte_sto) as float64) as prix_unitaire,
    r.is_derniere_photo_mois,
    r.is_photo_courante,
    st.extracted_at
from {{ ref('stg_mssql_sage__f_artstock') }} as st
inner join reperes as r
    on st.extracted_at = r.extracted_at
left join {{ ref('stg_mssql_sage__f_depot') }} as dep
    on st.de_no = dep.de_no
left join {{ ref('stg_mssql_sage__f_article') }} as ar
    on st.ar_ref = ar.ar_ref
where
    (r.is_derniere_photo_mois or r.is_photo_courante)
    and (st.as_qte_sto != 0 or st.as_mont_sto != 0)
