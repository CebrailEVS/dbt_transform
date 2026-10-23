{{ config(materialized='table') }}

-- La vue Oracle `model` n'est pas répliquée : un modèle d'équipement est un produit de type 4,
-- et device.idmodel pointe directement product.idproduct.
with modeles as (
    select *
    from {{ ref('stg_oracle_neshu__product') }}
    where
        idproduct_type = 4 -- MODELE : modèle d'équipement
        and code_status_record <> -1 -- exclut les ghost-deletes ERP
),

-- Seul label porté par les modèles.
isactive as (
    select
        lhp.idproduct,
        max(la.code) as isactive
    from {{ ref('stg_oracle_neshu__label_has_product') }} as lhp
    inner join {{ ref('stg_oracle_neshu__label') }} as la
        on lhp.idlabel = la.idlabel
    inner join {{ ref('stg_oracle_neshu__label_family') }} as lf
        on la.idlabel_family = lf.idlabel_family
    where lf.code = 'ISACTIVE'
    group by lhp.idproduct
),

suppliers as (
    select
        idcompany,
        name
    from {{ ref('stg_oracle_neshu__company') }}
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
    {{ utc_vers_paris_affichage('m.created_at') }} as created_at,
    {{ utc_vers_paris_affichage('m.updated_at') }} as updated_at

from modeles as m
left join isactive as a
    on m.idproduct = a.idproduct
left join suppliers as s
    on m.idcompany_supplier = s.idcompany
