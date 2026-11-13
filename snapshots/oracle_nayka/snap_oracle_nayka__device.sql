-- Historique SCD2 du parc d'équipements Nayka, lu depuis int_oracle_nayka__device.
-- Stratégie check sur toutes les colonnes sélectionnées : une version par changement
-- réel (dépôt ou client, propriétaire, statut, coût d'achat, prix du modèle…). La date
-- de modification de la fiche est exclue : elle créerait une version sans changement.
-- dbt_valid_from est l'heure d'exécution du snapshot, pas la date métier du changement.

{% snapshot snap_oracle_nayka__device %}

{{
    config(
      unique_key='device_id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
      tags=['oracle_nayka']
    )
}}

    select
        device_id,
        device_code,
        device_name,
        serial_number,
        device_type_id,
        parent_device_id,
        model_id,
        model_code,
        model_name,
        model_purchase_unit_price,
        company_id,
        company_code,
        company_name,
        company_type_id,
        owner_company_id,
        owner_company_name,
        owner_company_type_id,
        is_active,
        is_reserved,
        device_category,
        device_brand,
        device_range,
        device_state,
        device_material_status,
        purchase_date,
        purchase_cost,
        last_installation_date,
        created_at
    from {{ ref('int_oracle_nayka__device') }}

{% endsnapshot %}
