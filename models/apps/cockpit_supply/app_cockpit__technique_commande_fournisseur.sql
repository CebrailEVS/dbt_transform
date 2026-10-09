{{ config(materialized='table', cluster_by=['numero']) }}

-- Lignes des bons de commande fournisseurs TechCare (API Yuman), pour Cockpit Supply : remplacent
-- l'import manuel « Bons de commande fournisseurs ». Noms de colonnes de la table d'import de l'app
-- (sauf date → date_commande) ; dates en heure de Paris, numéro sans zéros de tête, statuts de l'export.
select
    purchase_order_line_id,
    purchase_order_id,
    coalesce(cast(safe_cast(purchase_order_number as int64) as string), purchase_order_number) as numero,
    date(creation_date, 'Europe/Paris') as date_commande,
    case purchase_order_status
        when 'Confirmed' then 'A recevoir'
        when 'Goods partially received' then 'Partiellement reçu'
        when 'Goods fully received' then 'Totalement reçu'
        when 'In preparation' then 'En préparation'
        else purchase_order_status
    end as statut_commande,
    purchase_order_invoice_status as statut_facturation,
    supplier_id,
    date(expected_delivery_date, 'Europe/Paris') as date_livraison_prevue,
    delivery_address as adresse_livraison,
    line_reference as reference,
    line_description as description,
    product_id,
    quantity as qte,
    unit as unite,
    quantity_received as qte_recue,
    unit_price as prix,
    line_subtotal as sous_total,
    subtotal_received_eur as sous_total_recu,
    line_updated_at as ligne_modifiee_le,
    extracted_at
from {{ ref('stg_yuman__purchase_orders') }}
where purchase_order_line_id is not null
