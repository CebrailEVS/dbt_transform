{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        partition_by={'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'month'},
        cluster_by=['idtask_type', 'idhistoric_action', 'idtask'],
        on_schema_change='append_new_columns',
        description='Historique des actions sur les tâches depuis evs_task_historic : une ligne par action, en ajout seul'
    )
}}

-- Pas de unique_key : la source écrit des doublons exacts légitimes, qu'un merge sur une clé
-- écraserait. Le raw ne fait qu'ajouter des lignes, le staging aussi.

with chargements_complets as (
    -- Un run dlt qui échoue après le chargement de la table y laisse des lignes que sa relance
    -- réajoute : seules celles des chargements complets sont lues.
    select load_id
    from {{ source('oracle_neshu', '_dlt_loads') }}
    where status = 0
),

source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_task_historic') }}
    where _dlt_load_id in (select cc.load_id from chargements_complets as cc)
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT. L'ERP écrit des NUMBER corrompus (octets nuls) sur des synchros
        -- EasyMobile, que le raw rend en NaN : safe_cast les vide plutôt que d'échouer.
        safe_cast(idtask as int64) as idtask,
        safe_cast(idhistoric_action as int64) as idhistoric_action,
        safe_cast(idtask_status as int64) as idtask_status,
        safe_cast(idtask_type as int64) as idtask_type,
        safe_cast(idtask_source as int64) as idtask_source,
        safe_cast(iduser as int64) as iduser,
        safe_cast(idtreatment_by_batch as int64) as idtreatment_by_batch,
        safe_cast(idinstance as int64) as idinstance,
        safe_cast(iddevice as int64) as iddevice,
        safe_cast(idcompany_peer as int64) as idcompany_peer,

        -- Colonnes texte
        nullif(trim(document_number), '') as document_number,
        nullif(trim(resources), '') as resources,
        nullif(trim(custom_data), '') as custom_data,

        -- Dates réelles de la tâche au moment de l'action
        {{ paris_vers_utc('real_start_date') }} as real_start_date,
        {{ paris_vers_utc('real_end_date') }} as real_end_date,

        -- Timestamps harmonisés : la ligne naît à l'action et ne change plus
        {{ paris_vers_utc('modification_date') }} as created_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
{% if is_incremental() %}
    -- Pas de fenêtre de rattrapage comme sur task : sans clé, une ligne relue serait doublée. Pour
    -- la même raison, un rechargement complet du raw impose un --full-refresh de ce modèle.
    where
        extracted_at > (
            select max(t.extracted_at)
            from {{ this }} as t
        )
{% endif %}
