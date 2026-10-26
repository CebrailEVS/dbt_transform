

-- Référentiel des articles Cafés du Phare pour Cockpit Supply, construit depuis le staging.
-- Périmètre : produits de type 1 et 5 ayant une date de création (celle du produit 1 est reprise de sa modification).

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
)

select
    p.idproduct as product_id,
    p.code as product_code,
    p.name as product_name,
    lb.product_family,
    lb.product_group,
    lb.product_bio,
    lb.product_en_exploitation,
    coalesce(lower(lb.is_active) = 'yes', false) as is_active
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product` as p
left join labels as lb
    on p.idproduct = lb.idproduct
where
    p.idproduct_type in (1, 5)
    and (case when p.idproduct = 1 and p.created_at is null then p.updated_at else p.created_at end) is not null