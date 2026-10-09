{{ config(materialized='table', cluster_by=['numero']) }}

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
from {{ ref('stg_yuman__purchase_orders') }} as po
left join {{ ref('stg_yuman__suppliers') }} as fournisseurs
    on po.supplier_id = fournisseurs.supplier_id
where po.purchase_order_line_id is not null
