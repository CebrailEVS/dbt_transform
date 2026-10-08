
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_ordre_production`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Pr\u00e9parations de fabrication Nunshen ouvertes (ordres de conditionnement Wissous pas encore r\u00e9alis\u00e9s) : un produit d\u00e9j\u00e0 programm\u00e9 n'est pas re-signal\u00e9 \u00ab \u00e0 produire \u00bb.\n[COMMENT CONSTRUITE] Lignes de pr\u00e9parations de fabrication (type 24) de stg_mssql_sage__f_docligne, quantit\u00e9s somm\u00e9es par pi\u00e8ce et r\u00e9f\u00e9rence. Le chargement Sage en remplacement ne garde que les pr\u00e9parations pas encore transform\u00e9es en bon de fabrication.\n[GRAIN] 1 ligne par (n_piece, reference).\n[NOTES] Remplace l'import \u00ab Listes des PF \u00bb (table Supabase production_wissous), o\u00f9 toutes les PF import\u00e9es restaient \u00ab en cours \u00bb faute de date de fabrication. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Préparations de fabrication Nunshen ouvertes (Sage, type 24) pour Cockpit Supply :
-- remplace l'import « Production Wissous ». Chargement Sage en remplacement : une
-- préparation transformée en bon de fabrication disparaît, il ne reste que les ouvertes.
select
    dl.do_piece as n_piece,
    date(dl.do_date) as date_document,
    dl.ar_ref as reference,
    ar.ar_design as designation,
    cast(sum(dl.dl_qte) as float64) as quantite,
    max(dl.extracted_at) as extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
    on dl.ar_ref = ar.ar_ref
where dl.do_type = 24 and dl.ar_ref is not null
group by n_piece, date_document, reference, designation
    );
  