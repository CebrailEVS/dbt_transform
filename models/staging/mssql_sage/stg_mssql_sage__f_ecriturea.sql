{{
    config(
        materialized='incremental',
        unique_key='cb_marq',
        incremental_strategy='merge',
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['ec_no'],
        description='Écritures analytiques nettoyées issues du système MSSQL Sage'
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_ecriturea') }}
),

cleaned_data as (
    select
        -- IDs et numéro convertis en BIGINT
        cast(cb_marq as int64) as cb_marq,
        cast(ec_no as int64) as ec_no,
        cast(n_analytique as int64) as n_analytique,
        cast(ea_ligne as int64) as ea_ligne,

        -- Colonnes texte
        ca_num,
        cb_createur,
        cb_creation_user,

        -- Colonnes numériques avec gestion des valeurs nulles
        ea_montant,
        ea_quantite,

        -- Timestamps harmonisés
        timestamp(cb_creation) as created_at,
        timestamp(coalesce(cb_modification, cb_creation)) as updated_at, -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select *
from cleaned_data