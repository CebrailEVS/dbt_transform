{{ config(materialized='table') }}

-- Articles Neshu avec leurs labels et leur typologie standardisée, pour la chaîne supply de l'app Cockpit.
-- Même logique que dim_neshu__product, dupliquée : l'app ne lit pas les marts.
with labels as (
    select
        lhp.idproduct,
        max(case when lf.code = 'MARQUEP' then l.code end) as product_brand,
        max(case when lf.code = 'PROPRIETAIRE' then l.code end) as product_owner,
        max(case when lf.code = 'FAMILLE' then l.code end) as product_family,
        max(case when lf.code = 'BIO' then l.code end) as product_bio,
        max(case when lf.code = 'PLANOETE' then l.code end) as product_planoete,
        max(case when lf.code = 'PLANOHIVER' then l.code end) as product_planohiver,
        max(case when lf.code = 'HPALME' then l.code end) as product_hpalme,
        max(case when lf.code = 'CLASSABC' then l.code end) as product_classabc,
        max(case when lf.code = 'EXPLOIT' then l.code end) as product_exploit,
        max(case when lf.code = 'GROUPE' then l.code end) as product_group,
        max(case when lf.code = 'LPTYPE' then l.code end) as product_type_raw,
        max(case when lf.code = 'ISACTIVE' then l.code end) as is_active
    from {{ ref('stg_oracle_neshu__label_has_product') }} as lhp
    inner join {{ ref('stg_oracle_neshu__label') }} as l
        on lhp.idlabel = l.idlabel
    inner join {{ ref('stg_oracle_neshu__label_family') }} as lf
        on l.idlabel_family = lf.idlabel_family
    group by lhp.idproduct
),

typed as (
    select
        p.idproduct as product_id,
        p.idproduct_type as product_type_id,
        p.code as product_code,
        p.name as product_name,
        p.purchase_unit_price,
        lb.product_brand,
        lb.product_owner,
        lb.product_family,
        lb.product_bio,
        lb.product_planoete,
        lb.product_planohiver,
        lb.product_hpalme,
        lb.product_classabc,
        lb.product_exploit,
        lb.product_group,
        coalesce(lower(lb.is_active) = 'yes', false) as is_active,
        -- Typologie standardisée ; le produit 1 est un produit technique « indéfini ».
        coalesce(
            case
                when p.idproduct = 1 then 'INDEFINI'
                when lb.product_family in ('CAFE CAPSULES', 'CAFE CAPSULES PREMIUM') then 'CAFE CAPS'
                when lb.product_family in ('THE') then 'THE'
                when lb.product_group = 'ACCESSOIRES' then 'ACCESSOIRES'
                when lb.product_group = 'BOISSONS FRAICHES' then 'BOISSONS FRAICHES'
                when lb.product_group = 'SNACKING' then 'SNACKING'
                when p.code = 'VANHCHOC23' then 'CHOCOLATS VAN HOUTEN'
                when lb.product_type_raw = 'BGOURMANDE' then 'BOISSONS GOURMANDES'
            end,
            nullif(trim(lb.product_type_raw), ''),
            'Non renseigné'
        ) as product_type
    from {{ ref('stg_oracle_neshu__product') }} as p
    left join labels as lb
        on p.idproduct = lb.idproduct
    where
        p.idproduct_type in (1, 5)
        and p.code_status_record <> -1  -- ghost-deletes ERP
        and (case when p.idproduct = 1 and p.created_at is null then p.updated_at else p.created_at end) is not null
),

purchase_unit as (
    -- Unité d'achat = conditionnement de commande fournisseur (idunit_type = 1 active) ; coeff_multi = nb
    -- d'unités de base par conditionnement. Unique par produit selon la source ; max() par prudence.
    select
        idproduct as product_id,
        max(coeff_multi) as purchase_unit_coeff,
        max(code) as purchase_unit_code
    from {{ ref('stg_oracle_neshu__product_unit') }}
    where idunit_type = 1 and isactive = 1
    group by idproduct
)

-- Un article « indéfini » a tous ses attributs forcés à INDEFINI.
select
    t.product_id,
    t.product_type_id,
    t.product_code,
    t.product_name,
    t.purchase_unit_price,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_brand) as product_brand,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_owner) as product_owner,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_family) as product_family,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_bio) as product_bio,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_planoete) as product_planoete,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_planohiver) as product_planohiver,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_hpalme) as product_hpalme,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_classabc) as product_classabc,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_exploit) as product_exploit,
    if(t.product_type = 'INDEFINI', 'INDEFINI', t.product_group) as product_group,
    t.product_type,
    t.is_active,
    pu.purchase_unit_coeff,
    pu.purchase_unit_code
from typed as t
left join purchase_unit as pu
    on t.product_id = pu.product_id
