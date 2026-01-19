-- ==============================================================================
-- SNAPSHOT: Oracle Neshu Company Dimension History
-- ==============================================================================
-- Source: dim_oracle_neshu__company
-- Purpose: Track historical changes (modèle économique, statut, organisation)
-- Strategy: Check - Only creates new records when tracked columns change
-- Tracked columns: proadman, company_economic_model, is_active
-- ==============================================================================

{% snapshot snap_oracle_neshu__company %}

{{
    config(
        unique_key='company_id',
        strategy='check',
        check_cols=[
            'proadman',
            'company_economic_model',
            'is_active'
        ],
        invalidate_hard_deletes=true,
        tags=['oracle_neshu']
    )
}}

WITH source_table AS (

    SELECT *
    FROM {{ ref('dim_oracle_neshu__company') }}

)

SELECT
    -- Identité
    company_id,
    company_type_id,
    company_code,
    company_name,
    company_type,

    -- Organisation / classification
    region,
    sector,
    sector_code,
    activity_sector,
    employee_range,

    -- Modèle économique & relation client
    company_economic_model,
    client_status,
    key_account,

    -- Offres / options
    katiers,
    remote_work,
    proadman,
    gsm,
    badge,
    recycling,

    -- Statut
    is_active,

    -- Localisation
    address1,
    address2,
    city,
    postal_code,
    country,

    -- Métadonnées source
    created_at,
    updated_at

FROM source_table

{% endsnapshot %}
