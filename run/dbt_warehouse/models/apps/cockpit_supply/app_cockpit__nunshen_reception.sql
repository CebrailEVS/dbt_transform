
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_reception`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9ceptions fournisseurs Nunshen (bons de r\u00e9ception) avec la commande d'origine et le lot re\u00e7u : achats, fiches et taux de service fournisseurs, flux, Audit Bio.\n[COMMENT CONSTRUITE] Lignes de bons de r\u00e9ception (type 13) de stg_mssql_sage__f_docligne ; d\u00e9p\u00f4t et date de livraison de la ligne, sinon de l'en-t\u00eate ; fournisseur de stg_mssql_sage__f_comptet ; lot de la photo la plus r\u00e9cente de stg_mssql_sage__f_lotserie (ligne d'entr\u00e9e li\u00e9e par dl_no_in).\n[GRAIN] 1 ligne par ligne de r\u00e9ception Sage, depuis janvier 2024.\n[NOTES] Remplace l'import \u00ab R\u00e9ceptions fournisseurs \u00bb (lignes \u00ab Bon de livraison \u00bb), qui \u00e9crasait les lignes d'un m\u00eame produit re\u00e7u en plusieurs lots. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Réceptions fournisseurs Nunshen (Sage, bons de réception type 13) pour Cockpit Supply :
-- remplace l'import « Réceptions fournisseurs » (lignes « Bon de livraison »). Une ligne
-- par ligne de réception : un produit reçu en plusieurs lots garde toutes ses lignes
-- (l'import les écrasait sur n° de bon × référence). Lot reçu (n°, dates) : photo la plus
-- récente de f_lotserie, une seule ligne d'entrée par ligne de réception. Depuis 2024.
with derniere_photo_lots as (
    select max(extracted_at) as extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
),
lots as (
    select
        ls.dl_no_in,
        ls.ls_no_serie,
        ls.ls_fabrication,
        ls.ls_peremption
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie` as ls
    inner join derniere_photo_lots as d
        on ls.extracted_at = d.extracted_at
    where ls.ls_mvt_stock = 1
    qualify row_number() over (partition by ls.dl_no_in order by ls.ls_qte desc) = 1
)
select
    dl.do_piece as n_piece,
    dl.dl_piece_bc as n_piece_bc,
    date(dl.dl_date_bc) as date_piece_bc,
    date(dl.do_date) as date_piece_bl,
    date(coalesce(dl.do_date_livr, de.do_date_livr)) as date_livraison,
    dl.ar_ref as reference,
    coalesce(ar.ar_design, dl.dl_design) as designation,
    cast(dl.dl_qte as float64) as qte_livree,
    cast(dl.dl_qte_bc as float64) as qte_commandee,
    cast(dl.dl_prix_unitaire as float64) as prix_unitaire_ht,
    cast(dl.dl_montant_ht as float64) as montant_ht,
    dep.de_intitule as depot,
    trim(dl.ct_num) as code_fournisseur,
    trim(ct.ct_intitule) as intitule_fournisseur,
    l.ls_no_serie as n_serie_lot,
    date(l.ls_fabrication) as date_fabrication,
    date(l.ls_peremption) as date_peremption,
    dl.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docentete` as de
    on dl.do_type = de.do_type and dl.do_piece = de.do_piece
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot` as dep
    on coalesce(nullif(dl.de_no, 0), de.de_no) = dep.de_no
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
    on dl.ar_ref = ar.ar_ref
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptet` as ct
    on dl.ct_num = ct.ct_num
left join lots as l
    on dl.dl_no = l.dl_no_in
where
    dl.do_type = 13
    and dl.ar_ref is not null
    and dl.do_date >= timestamp('2024-01-01')
    );
  