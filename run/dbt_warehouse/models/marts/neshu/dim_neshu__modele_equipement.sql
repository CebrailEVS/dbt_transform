
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_neshu__modele_equipement`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension des mod\u00e8les d'\u00e9quipement Neshu : mod\u00e8les de distributeurs, de terminaux de paiement et de mobilier (ex. MOMENTO 100, NAYAX ONYX, MEUBLE NESHU). Sert de FK aux faits qui portent une machine.\n[COMMENT CONSTRUITE] stg_oracle_neshu__product filtr\u00e9 sur le type 4 (MODELE), hors ghost-deletes ERP (code_status_record = -1). La vue Oracle `model` n'est pas r\u00e9pliqu\u00e9e : device.idmodel pointe directement product.idproduct. Statut actif lu dans le label ISACTIVE (seul label des mod\u00e8les), nom du fournisseur lu dans stg_oracle_neshu__company. Instants en heure de Paris.\n[GRAIN] 1 ligne par modele_equipement_id (produit de type 4). 139 mod\u00e8les en octobre 2026.\n[NOTES] Le type de machine (distributeur, terminal de paiement, accessoire\u2026) reste dans dim_neshu__device. En pratique un mod\u00e8le correspond \u00e0 un seul type, \u00e0 une exception pr\u00e8s (BASE CABINET REFILL, 2 machines de types diff\u00e9rents). device_name de dim_neshu__device reprend le plus souvent le nom du mod\u00e8le.\n"""
    )
    as (
      

-- La vue Oracle `model` n'est pas répliquée : un modèle d'équipement est un produit de type 4,
-- et device.idmodel pointe directement product.idproduct.
with modeles as (
    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product`
    where
        idproduct_type = 4 -- MODELE : modèle d'équipement
        and code_status_record <> -1 -- exclut les ghost-deletes ERP
),

-- Seul label porté par les modèles.
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
    m.idproduct as modele_equipement_id,
    m.idcompany_supplier as supplier_company_id,
    m.code as modele_equipement_code,
    m.name as modele_equipement_name,
    nullif(trim(m.commercial_name), '') as modele_equipement_commercial_name,
    s.name as supplier_name,
    coalesce(lower(a.isactive) = 'yes', false) as is_active,
    m.purchase_unit_price,
    timestamp(datetime(m.created_at, 'Europe/Paris')) as created_at,
    timestamp(datetime(m.updated_at, 'Europe/Paris')) as updated_at

from modeles as m
left join isactive as a
    on m.idproduct = a.idproduct
left join suppliers as s
    on m.idcompany_supplier = s.idcompany
    );
  