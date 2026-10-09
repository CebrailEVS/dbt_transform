

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