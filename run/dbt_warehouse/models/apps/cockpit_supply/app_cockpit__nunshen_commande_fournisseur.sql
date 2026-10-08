
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_commande_fournisseur`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Lignes de commandes fournisseurs Nunshen encore ouvertes : reste \u00e0 livrer, date de livraison pr\u00e9vue, d\u00e9p\u00f4t de r\u00e9ception, fournisseur. Alimente le stock projet\u00e9, les commandes Siti, le cash flow, les retards fournisseurs et le statut \u00ab en attente de validation mati\u00e8re \u00bb.\n[COMMENT CONSTRUITE] Lignes de commandes fournisseurs (type 12) de stg_mssql_sage__f_docligne ; d\u00e9p\u00f4t et date de livraison de la ligne, sinon de l'en-t\u00eate ; fournisseur de stg_mssql_sage__f_comptet (intitul\u00e9 sans espaces finaux) ; quantit\u00e9 d\u00e9j\u00e0 re\u00e7ue somm\u00e9e sur les r\u00e9ceptions de la m\u00eame commande et de la m\u00eame r\u00e9f\u00e9rence.\n[GRAIN] 1 ligne par ligne de commande ouverte (n_piece, reference, une ligne Sage).\n[NOTES] Remplace l'import \u00ab Commandes fournisseurs \u00bb (lignes \u00ab Bon de commande \u00bb). Sage supprime une ligne re\u00e7ue de la commande : qte_restante = quantit\u00e9 de la ligne (r\u00e8gle nun_qte_restante de l'app). R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Commandes fournisseurs Nunshen ouvertes (Sage, type 12) pour Cockpit Supply : remplace
-- l'import « Commandes fournisseurs » (lignes « Bon de commande »). Chargement Sage en
-- remplacement : une ligne reçue disparaît de la commande, la quantité d'une ligne
-- ouverte est donc déjà le reste à livrer (qte_restante, lue par nun_qte_restante).
-- qte_livree = quantité déjà reçue sur la même commande et la même référence.
with receptions as (
    select
        dl_piece_bc,
        ar_ref,
        sum(dl_qte) as qte_livree
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
    where do_type = 13 and dl_piece_bc is not null and ar_ref is not null
    group by dl_piece_bc, ar_ref
)
select
    'Bon de commande' as type_document,
    dl.do_piece as n_piece,
    dl.ar_ref as reference,
    coalesce(ar.ar_design, dl.dl_design) as designation,
    cast(dl.dl_qte as float64) as qte_commandee,
    cast(dl.dl_qte as float64) as qte_restante,
    cast(coalesce(r.qte_livree, 0) as float64) as qte_livree,
    cast(dl.dl_prix_unitaire as float64) as prix_unitaire_ht,
    date(dl.do_date) as date_piece_bc,
    date(coalesce(dl.do_date_livr, de.do_date_livr)) as date_livraison,
    dep.de_intitule as depot,
    trim(dl.ct_num) as code_fournisseur,
    trim(ct.ct_intitule) as intitule_fournisseur,
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
left join receptions as r
    on dl.do_piece = r.dl_piece_bc and dl.ar_ref = r.ar_ref
where dl.do_type = 12 and dl.ar_ref is not null
    );
  