
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_vente_mensuelle`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Ventes Nunshen par mois et par r\u00e9f\u00e9rence : quantit\u00e9s et chiffre d'affaires HT. Base des pr\u00e9visions, de la disponibilit\u00e9, de l'ABC/XYZ et des KPI Wissous.\n[COMMENT CONSTRUITE] Lignes de factures et d'avoirs de vente (do_domaine 0, types 6 et 7) de stg_mssql_sage__f_docligne, hors composants de kits (dl_valorise = 1), somm\u00e9es par mois de facture ; quantit\u00e9 d'un avoir financier (quantit\u00e9 n\u00e9gative sans retour en stock) non d\u00e9duite, comme l'export \u00ab Ventes \u00bb ; d\u00e9signation et famille de l'article.\n[GRAIN] 1 ligne par (annee, mois, reference), depuis janvier 2024 (filtre du mod\u00e8le).\n[NOTES] Les avoirs sont d\u00e9j\u00e0 n\u00e9gatifs dans Sage. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Ventes Nunshen par mois et référence (Sage) pour Cockpit Supply.
-- Factures (types 6 et 7, avoirs compris, quantités déjà signées), lignes
-- valorisées seulement (dl_valorise = 1 : sans les composants de kits, qui doubleraient
-- le CA). Référence brute (alias appliqués par l'app).
select
    extract(year from dl.do_date) as annee,
    extract(month from dl.do_date) as mois,
    dl.ar_ref as reference,
    ar.ar_design as designation,
    ar.fa_code_famille as code_famille,
    -- Avoir financier (quantité négative sans retour en stock) : CA déduit, quantité non
    -- (règle de l'export « Ventes » de Sage)
    cast(sum(if(dl.dl_qte < 0 and dl.dl_mvt_stock = 0, 0, dl.dl_qte)) as float64) as qte_vendue,
    cast(sum(dl.dl_montant_ht) as float64) as ca_ht,
    max(dl.extracted_at) as extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
    on dl.ar_ref = ar.ar_ref
where
    dl.do_domaine = 0
    and dl.do_type in (6, 7)
    and dl.dl_valorise = 1
    and dl.ar_ref is not null
    and dl.do_date >= timestamp('2024-01-01')
group by annee, mois, reference, designation, code_famille
    );
  