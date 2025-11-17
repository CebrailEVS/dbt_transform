{{
    config(
        materialized='incremental',
        unique_key='idtask',
        partition_by={'field': 'real_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge', 
        cluster_by=['idtask_type','idtask_status','idcompany_peer','iddevice'],
        description='Table de fait des tâches depuis la table evs_task'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_task') }}
),

cleaned_data as (
    select
        -- IDs (clés primaires et étrangères)
        cast(idtask as int64) as idtask,
        cast(task_idtask as int64) as task_idtask,
        cast(idtask_type as int64) as idtask_type,
        cast(idtask_status as int64) as idtask_status,
        cast(iddevice as int64) as iddevice,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idlocation as int64) as idlocation,
        cast(idcontact as int64) as idcontact,
        cast(idproduct_source as int64) as idproduct_source,
        cast(idproduct_destination as int64) as idproduct_destination,

        -- Colonnes texte et types
        type_product_source,
        type_product_destination,

        -- Colonne numérique
        spantime as spantime, -- durée de la tâche en minute
        CAST(code_status_record AS STRING) as code_status_record, -- status record (1 = validé, 0 ou -1 possiblement inactif/desactivé : à confirmer)
        
        -- Date de la tache
        timestamp(real_start_date) as real_start_date,
        timestamp(real_end_date) as real_end_date,
        timestamp(planed_start_date) as planed_start_date,
        timestamp(planed_end_date) as planed_end_date,
        
        -- Timestamps harmonisés
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at, -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

{% if is_incremental() %}
WHERE
    (
        updated_at > (
            SELECT MAX(updated_at)
            FROM {{ this }}
        )
        OR updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )
{% endif %}