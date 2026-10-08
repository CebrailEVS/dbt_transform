{{ config(materialized='table') }}

-- Lots en stock Nunshen (Sage, dernière photo de f_lotserie) pour Cockpit Supply :
-- péremption et Audit Bio. Lots d'entrée (ls_mvt_stock = 1) encore ouverts, regroupés par
-- dépôt, référence et n° de lot. Seuls les articles suivis par lot y figurent : le stock
-- disponible se lit dans app_cockpit__nunshen_stock_photo.
with derniere as (
    select max(extracted_at) as extracted_at
    from {{ ref('stg_mssql_sage__f_lotserie') }}
)
select
    date(ls.extracted_at, 'Europe/Paris') as stock_date,
    ls.de_no as depot_id,
    dep.de_intitule as depot,
    ls.ar_ref as reference,
    ar.ar_design as designation,
    ar.fa_code_famille as code_famille,
    ls.ls_no_serie as n_serie,
    min(date(ls.ls_fabrication)) as date_fabrication,
    min(date(ls.ls_peremption)) as date_peremption,
    cast(sum(ls.ls_qte_restant) as float64) as stock_reel,
    max(ls.extracted_at) as extracted_at
from {{ ref('stg_mssql_sage__f_lotserie') }} as ls
inner join derniere as d
    on ls.extracted_at = d.extracted_at
left join {{ ref('stg_mssql_sage__f_depot') }} as dep
    on ls.de_no = dep.de_no
left join {{ ref('stg_mssql_sage__f_article') }} as ar
    on ls.ar_ref = ar.ar_ref
where ls.ls_mvt_stock = 1 and ls.ls_qte_restant > 0
group by stock_date, depot_id, depot, reference, designation, code_famille, n_serie
