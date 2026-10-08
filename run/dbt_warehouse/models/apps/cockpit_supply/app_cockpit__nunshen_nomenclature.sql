
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_nomenclature`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Nomenclature de niveau 1 des produits Nunshen : composants directs (vrac, poche, bo\u00eete, \u00e9tiquette) et quantit\u00e9 par produit. Sert \u00e0 v\u00e9rifier les emballages vides avant de signaler un produit \u00ab \u00e0 produire \u00bb.\n[COMMENT CONSTRUITE] stg_mssql_sage__f_nomenclat, d\u00e9signation et famille du produit et d\u00e9signation du composant lues dans stg_mssql_sage__f_article.\n[GRAIN] 1 ligne par (reference, composant_reference).\n[NOTES] Remplace l'import \u00ab Nomenclature \u00bb (table Supabase nomenclature), sans les doublons de l'export, avec la quantit\u00e9 par composant en plus. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Nomenclatures Nunshen (Sage, niveau 1) pour Cockpit Supply : remplace l'import
-- « Nomenclature », avec en plus la quantité de chaque composant.
select
    nm.ar_ref as reference,
    p.ar_design as designation,
    p.fa_code_famille as code_famille,
    nm.no_ref_det as composant_reference,
    c.ar_design as composant_designation,
    cast(nm.no_qte as float64) as quantite_composant,
    nm.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_nomenclat` as nm
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as p
    on nm.ar_ref = p.ar_ref
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as c
    on nm.no_ref_det = c.ar_ref
    );
  