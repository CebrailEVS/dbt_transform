-- ==============================================================================
-- SNAPSHOT: Oracle Neshu Valorisation Parc Machines - Historique Mensuel
-- ==============================================================================
-- Source: int_oracle_neshu__valorisation_parc_machines
-- Purpose: Capture l'état mensuel de la valorisation du parc machines NESHU
--          pour permettre des comparaisons mois par mois
-- Strategy: Timestamp - Crée un nouvel enregistrement uniquement au changement de mois
-- Tracked column: snapshot_month (DATE_TRUNC du mois en cours)
-- Frequency: Run quotidien mais snapshot mensuel automatique
-- Note: Résiste aux full-refresh hebdomadaires de la table source

{% snapshot snap_oracle_neshu__valo_parc_machines %}

{{
  config(
    target_schema='snapshots',
    unique_key='snapshot_month || "|" || device_name || "|" || device_group',
    strategy='timestamp',
    updated_at='snapshot_month',
    invalidate_hard_deletes=True
  )
}}

SELECT 
  TIMESTAMP(DATE_TRUNC(CURRENT_DATE(), MONTH)) as snapshot_month,  -- ✅ Ajout de TIMESTAMP()
  device_name,
  device_group,
  nombre_machines,
  valorisation_totale_machine
FROM {{ ref('int_oracle_neshu__valorisation_parc_machines') }}

{% endsnapshot %}