
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_commande_fournisseur`
      
    
    cluster by numero

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Lignes des bons de commande fournisseurs TechCare (Yuman), telles que lues par l'application Cockpit Supply : commandes en cours, achats et taux de service des fiches fournisseurs, retards, ISO.\n[COMMENT CONSTRUITE] stg_yuman__purchase_orders joint \u00e0 stg_yuman__suppliers, sans filtre de date (historique depuis 2024). Colonnes renomm\u00e9es comme la table d'import de l'app qu'elle remplace (import manuel \u00ab Bons de commande fournisseurs \u00bb) ; num\u00e9ro sans z\u00e9ros de t\u00eate ; dates en heure de Paris ; statuts aux libell\u00e9s de l'export Yuman.\n[GRAIN] 1 ligne par `purchase_order_line_id` (ligne de bon de commande).\n[NOTES] Code et nom du fournisseur depuis stg_yuman__suppliers. Les r\u00e8gles de l'app (d\u00e9p\u00f4t d\u00e9duit de l'adresse de livraison, commandes en cours, exclusions) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Lignes des bons de commande fournisseurs TechCare (API Yuman), pour Cockpit Supply : remplacent
-- l'import manuel « Bons de commande fournisseurs ». Noms de colonnes de la table d'import de l'app
-- (sauf date → date_commande) ; dates en heure de Paris, numéro sans zéros de tête, statuts de l'export.
select
    po.purchase_order_line_id,
    po.purchase_order_id,
    coalesce(cast(safe_cast(po.purchase_order_number as int64) as string), po.purchase_order_number) as numero,
    date(po.creation_date, 'Europe/Paris') as date_commande,
    case po.purchase_order_status
        when 'Confirmed' then 'A recevoir'
        when 'Goods partially received' then 'Partiellement reçu'
        when 'Goods fully received' then 'Totalement reçu'
        when 'In preparation' then 'En préparation'
        else po.purchase_order_status
    end as statut_commande,
    po.purchase_order_invoice_status as statut_facturation,
    po.supplier_id,
    fournisseurs.supplier_code as code_fournisseur,
    fournisseurs.supplier_name as nom_fournisseur,
    date(po.expected_delivery_date, 'Europe/Paris') as date_livraison_prevue,
    po.delivery_address as adresse_livraison,
    po.line_reference as reference,
    po.line_description as description,
    po.product_id,
    po.quantity as qte,
    po.unit as unite,
    po.quantity_received as qte_recue,
    po.unit_price as prix,
    po.line_subtotal as sous_total,
    po.subtotal_received_eur as sous_total_recu,
    po.line_updated_at as ligne_modifiee_le,
    po.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__purchase_orders` as po
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__suppliers` as fournisseurs
    on po.supplier_id = fournisseurs.supplier_id
where po.purchase_order_line_id is not null
    );
  