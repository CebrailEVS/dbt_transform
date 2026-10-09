
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_bl_client`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Lignes de bons de livraison clients Nunshen avec leur date de pr\u00e9paration : \u00ab commandes trait\u00e9es \u00bb (BL distincts) et d\u00e9lai de traitement (BL \u2212 pr\u00e9paration) des KPI Wissous, de l'ISO et de la Vue d'ensemble Supply.\n[COMMENT CONSTRUITE] Lignes de vente de stg_mssql_sage__f_docligne portant un BL (factures et BL non factur\u00e9s, types 3, 6, 7), hors composants de kits et hors mouvements d'entr\u00e9e. date_pl laiss\u00e9e vide quand la ligne n'a pas de pr\u00e9paration (au lieu d'une date incoh\u00e9rente).\n[GRAIN] 1 ligne par ligne de document Sage (dl_no), depuis janvier 2024 (date du BL, filtre du mod\u00e8le).\n[NOTES] is_intragroupe marque les factures \u00ab ZZ \u00bb (demande m\u00e9tier 9.3, commandes exp\u00e9di\u00e9es avec ou sans intragroupe). R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

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
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
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
    );
  