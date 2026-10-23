
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_neshu__piece`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension des pi\u00e8ces d\u00e9tach\u00e9es Neshu : filtres \u00e0 eau, bouteilles de CO2, modules de distribution. Sert de FK aux faits qui portent une pi\u00e8ce (intervention technique).\n[COMMENT CONSTRUITE] stg_oracle_neshu__product filtr\u00e9 sur le type 2 (PIECE), hors ghost-deletes ERP (code_status_record = -1). Statut actif lu dans le label ISACTIVE (seul label des pi\u00e8ces), nom du fournisseur lu dans stg_oracle_neshu__company. Instants en heure de Paris.\n[GRAIN] 1 ligne par piece_id (produit de type 2). 9 pi\u00e8ces en octobre 2026.\n[NOTES] Les produits de types 1 et 5 sont dans dim_neshu__product, les mod\u00e8les d'\u00e9quipement (type 4) dans dim_neshu__modele_equipement. Les pi\u00e8ces sont encore peu suivies dans Oracle (une quarantaine de t\u00e2ches depuis mai 2025).\n"""
    )
    as (
      

with pieces as (
    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product`
    where
        idproduct_type = 2 -- PIECE : pièces détachées
        and code_status_record <> -1 -- exclut les ghost-deletes ERP
),

-- Seul label porté par les pièces.
isactive as (
    select
        lhp.idproduct,
        max(la.code) as isactive
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_product` as lhp
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as la
        on lhp.idlabel = la.idlabel
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on la.idlabel_family = lf.idlabel_family
    where lf.code = 'ISACTIVE'
    group by lhp.idproduct
),

suppliers as (
    select
        idcompany,
        name
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
)

select
    p.idproduct as piece_id,
    p.idcompany_supplier as supplier_company_id,
    p.code as piece_code,
    p.name as piece_name,
    nullif(trim(p.commercial_name), '') as piece_commercial_name,
    s.name as supplier_name,
    coalesce(lower(a.isactive) = 'yes', false) as is_active,
    p.purchase_unit_price,
    timestamp(datetime(p.created_at, 'Europe/Paris')) as created_at,
    timestamp(datetime(p.updated_at, 'Europe/Paris')) as updated_at

from pieces as p
left join isactive as a
    on p.idproduct = a.idproduct
left join suppliers as s
    on p.idcompany_supplier = s.idcompany
    );
  