-- ==============================================================================
-- SNAPSHOT: Yuman purchase order lines — quantités reçues
-- ==============================================================================
-- Source: stg_yuman__purchase_orders (une ligne = une ligne de bon de commande)
-- Purpose: dater les réceptions fournisseurs TechCare. L'API Yuman ne donne que l'état
--          courant (quantity_received) : chaque nouvelle version d'une ligne = une réception
--          vue ce jour-là (taux de service « livré en une fois », délai de livraison).
-- Strategy: Check sur quantity_received et purchase_order_status
--
-- Usage:
--   Réceptions : versions successives d'une ligne, delta de quantity_received entre deux versions
--   Query history: SELECT * FROM snapshots.snap_yuman__purchase_order_lines WHERE purchase_order_line_id = X ORDER BY dbt_valid_from
-- ==============================================================================

{% snapshot snap_yuman__purchase_order_lines %}

{{
    config(
      unique_key='purchase_order_line_id',
      strategy='check',
      check_cols=['quantity_received', 'purchase_order_status'],
      invalidate_hard_deletes=True,
      tags=['yuman']
    )
}}

    select
        purchase_order_line_id,
        purchase_order_id,
        purchase_order_number,
        purchase_order_status,      -- TRACKED
        supplier_id,
        line_reference,
        quantity,
        quantity_received,          -- TRACKED
        unit_price,
        creation_date,
        expected_delivery_date,
        line_updated_at,
        extracted_at
    from {{ ref('stg_yuman__purchase_orders') }}
    where purchase_order_line_id is not null

{% endsnapshot %}
