{{ config(materialized='table') }}

-- Lignes de BL clients Nunshen (Sage) pour Cockpit Supply (nombre de commandes = BL distincts,
-- délai de préparation = date du BL − date de préparation). Lignes de factures (types 6 et 7) et de
-- BL pas encore facturés (type 3, n° de BL = n° de pièce), hors avoirs, lignes valorisées.
-- Date de préparation laissée vide quand la ligne n'a pas de préparation (date non
-- fiable, délais négatifs).
with lignes as (
    select
        coalesce(dl.dl_piece_bl, dl.do_piece) as n_piece_bl,
        date(coalesce(dl.dl_date_bl, dl.do_date)) as date_piece_bl,
        if(dl.dl_piece_pl is not null, date(dl.dl_date_pl), null) as date_pl,
        dl.do_piece as n_piece,
        dl.ar_ref as reference,
        cast(dl.dl_qte as float64) as qte,
        starts_with(dl.do_piece, 'ZZ') as is_intragroupe,
        dl.do_type = 3 as is_non_facture,
        dl.extracted_at
    from {{ ref('stg_mssql_sage__f_docligne') }} as dl
    where
        dl.do_domaine = 0
        and dl.do_type in (3, 6, 7)
        and dl.ar_ref is not null
        and dl.dl_valorise = 1
        and dl.dl_mvt_stock != 1
        and (dl.dl_piece_bl is not null or dl.do_type = 3)
)
select
    extract(year from date_piece_bl) as annee,
    extract(month from date_piece_bl) as mois,
    reference,
    n_piece,
    n_piece_bl,
    date_piece_bl,
    date_pl,
    qte,
    is_intragroupe,
    is_non_facture,
    extracted_at
from lignes
where date_piece_bl >= date('2024-01-01')
