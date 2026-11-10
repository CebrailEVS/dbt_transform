

-- Référentiel des articles Cafés du Phare pour Cockpit Supply, construit depuis le staging.
-- Périmètre : articles de type 1 et 5 ayant une date de création (celle du produit 1 est reprise de sa
-- modification), et modèles de machines (type 4 MODELE), qui n'en ont souvent pas.

with labels as (
    -- Libellés FR, sauf ISACTIVE gardé sur le code (YES/NO) pour la conversion booléenne.
    select
        idproduct,
        max(case when label_family_code = 'FAMIPRO' then label_text_fr end) as product_family,
        -- Libellé saisi avec une espace finale sur un groupe (« ACCESSOIRE ») : rogné pour le regroupement.
        max(case when label_family_code = 'GROUPRO' then trim(label_text_fr) end) as product_group,
        max(case when label_family_code = 'BIO' then label_text_fr end) as product_bio,
        -- Champ « EN EXPLOITATION » : OUI = article stocké et suivi au quotidien ; NULL tant qu'il n'est pas saisi.
        max(case when label_family_code = 'ENEXPL' then trim(label_text_fr) end) as product_en_exploitation,
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_product`
    group by idproduct
),

produits as (
    select
        idproduct,
        code,
        name,
        idproduct_type
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product`
    where
        idproduct_type in (1, 5)
        and (case when idproduct = 1 and created_at is null then updated_at else created_at end) is not null
),

-- Modèles de machines : le code des machines du dépôt NEUF. Un modèle dont le code est déjà celui d'un
-- article (« INDEFINI ») est écarté, pour que product_code reste unique.
modeles as (
    select
        m.idproduct,
        m.code,
        m.name,
        m.idproduct_type
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product` as m
    where
        m.idproduct_type = 4
        and m.code is not null
        and not exists (select 1 from produits as pr where pr.code = m.code)
),

articles as (
    select
        idproduct,
        code,
        name,
        idproduct_type
    from produits
    union all
    select
        idproduct,
        code,
        name,
        idproduct_type
    from modeles
)

select
    p.idproduct as product_id,
    p.code as product_code,
    p.name as product_name,
    pt.code as product_type,
    lb.product_family,
    lb.product_group,
    lb.product_bio,
    lb.product_en_exploitation,
    coalesce(lower(lb.is_active) = 'yes', false) as is_active
from articles as p
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product_type` as pt
    on p.idproduct_type = pt.idproduct_type
left join labels as lb
    on p.idproduct = lb.idproduct