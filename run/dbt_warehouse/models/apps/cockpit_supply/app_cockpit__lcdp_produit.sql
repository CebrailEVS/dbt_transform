
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_produit`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des articles Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] stg_oracle_lcdp__product filtr\u00e9e comme la dim (types 1 et 5, date de cr\u00e9ation renseign\u00e9e), libell\u00e9s de stg_oracle_lcdp__label_product. Colonnes lues par l'app seulement.\n[GRAIN] 1 ligne par product_id.\n[NOTES] lue via get_dim_products_lcdp, get_produits_actifs_lcdp ; \u00e9crans : _flux, _pilotage, accueil, lcdp_distrib, lcdp_pilotage, lcdp_torrefaction, neshu_partenaires, nunshen_partenaires, supply, technique_partenaires. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

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
    );
  