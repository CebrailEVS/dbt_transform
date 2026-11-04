
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_prix_achat`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Prix d'achat des articles TechCare, tels que lus par l'application Cockpit Supply pour valoriser les mouvements et le stock.\n[COMMENT CONSTRUITE] stg_yuman__products (prix d'achat de la fiche article) ; un code article en double garde la fiche active la plus r\u00e9cemment modifi\u00e9e ; dernier prix de bon de commande (stg_yuman__purchase_orders) en repli. Remplace l'onglet \u00ab PU HA \u00bb de l'import manuel \u00ab Fichier ma\u00eetre prix \u00bb.\n[GRAIN] 1 ligne par `reference` (code article Yuman).\n[NOTES] Prix vivant (fiche article du jour) ; un prix fig\u00e9 en fin d'ann\u00e9e, si le pilote le retient pour la valorisation d'inventaire, se fera par snapshot ou seed annuel. Les r\u00e8gles de l'app (partenaires gratuits, prix Nespresso) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Prix d'achat des articles TechCare (catalogue Yuman), pour Cockpit Supply : remplace l'onglet
-- « PU HA » de l'import manuel « Fichier maître prix ». Un code article en double dans Yuman garde
-- la fiche active la plus récemment modifiée ; le dernier prix de bon de commande sert de repli.
with produits as (

    select
        product_code,
        product_id,
        product_name,
        product_purchase_price,
        is_active,
        updated_at,
        row_number() over (
            partition by product_code
            order by is_active desc, updated_at desc, product_id desc
        ) as rang
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__products`
    where product_code is not null

),

dernier_bdc as (

    select
        line_reference as product_code,
        unit_price,
        date(creation_date, 'Europe/Paris') as date_bdc,
        row_number() over (
            partition by line_reference
            order by creation_date desc, purchase_order_line_id desc
        ) as rang
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__purchase_orders`
    where line_reference is not null and unit_price > 0

)

select
    produits.product_code as reference,
    produits.product_id,
    produits.product_name as designation,
    produits.product_purchase_price as prix_achat,
    dernier_bdc.unit_price as dernier_prix_bdc,
    dernier_bdc.date_bdc as date_dernier_bdc,
    produits.is_active,
    produits.updated_at as fiche_modifiee_le
from produits
left join dernier_bdc
    on produits.product_code = dernier_bdc.product_code and dernier_bdc.rang = 1
where produits.rang = 1
    );
  