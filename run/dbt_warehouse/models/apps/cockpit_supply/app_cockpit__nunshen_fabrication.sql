
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_fabrication`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Produits fabriqu\u00e9s \u00e0 Wissous par bon de fabrication : \u00ab produits r\u00e9alis\u00e9s \u00bb des KPI Wissous et entr\u00e9e \u00ab production \u00bb du flux.\n[COMMENT CONSTRUITE] Lignes de bons de fabrication (type 26) de stg_mssql_sage__f_docligne qui entrent en stock (dl_mvt_stock = 1 ; 3 = composants consomm\u00e9s), hors r\u00e9f\u00e9rences de service MANUFACT, quantit\u00e9s somm\u00e9es par bon et r\u00e9f\u00e9rence.\n[GRAIN] 1 ligne par (n_piece, date_document, reference), depuis janvier 2024 (filtre du mod\u00e8le).\n[NOTES] Un produit fabriqu\u00e9 en plusieurs lots a une ligne par lot dans Sage : la somme \u00e9vite le sous-comptage. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Produits fabriqués Nunshen (Sage, bons de fabrication type 26) pour Cockpit Supply :
-- produits seulement (dl_mvt_stock = 1 ; 3 =
-- composants consommés), hors lignes MANUFACT ; quantité SOMMÉE par bon et référence (un
-- produit peut être découpé en une ligne par lot).
select
    dl.do_piece as n_piece,
    date(dl.do_date) as date_document,
    dl.ar_ref as reference,
    ar.fa_code_famille as code_famille,
    cast(sum(dl.dl_qte) as float64) as quantite,
    count(*) as nb_lignes_lot,
    max(dl.extracted_at) as extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
    on dl.ar_ref = ar.ar_ref
where
    dl.do_type = 26
    and dl.dl_mvt_stock = 1
    and dl.ar_ref is not null
    and not ends_with(dl.ar_ref, 'MANUFACT')
    and dl.do_date >= timestamp('2024-01-01')
group by n_piece, date_document, reference, code_famille
    );
  