

with historique as (
    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic`
    where
        -- Types techniques : l'essentiel du volume, sans action métier
        (idtask_type is null or idtask_type not in (3, 243)) -- AUDITTELE, SYS_EXEC
        -- Une action sans tâche (IDTASK corrompu dans l'ERP) ne raconte l'historique d'aucune tâche
        and idtask is not null
        
            and extracted_at > (
                select max(t.extracted_at)
                from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__task_historic_metier` as t
            )
        
),

actions as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__historic_action`
),

libelles as (
    select
        idstring,
        text
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__string`
    where langage_code = 'fr_FR'
),

statuts as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status`
),

types as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_type`
),

-- La source écrit des lignes identiques à la même seconde : une action distincte, et le
-- nombre de lignes qui la portent. L'ingestion ne lit une seconde qu'une fois complète, donc
-- en un seul chargement.
evenements as (
    select
        idtask,
        idhistoric_action,
        idtask_status,
        idtask_type,
        idtask_source,
        iduser,
        idtreatment_by_batch,
        idinstance,
        iddevice,
        idcompany_peer,
        document_number,
        resources,
        custom_data,
        real_start_date,
        real_end_date,
        created_at,
        count(*) as nb_lignes_source,
        max(extracted_at) as extracted_at
    from historique
    group by
        idtask,
        idhistoric_action,
        idtask_status,
        idtask_type,
        idtask_source,
        iduser,
        idtreatment_by_batch,
        idinstance,
        iddevice,
        idcompany_peer,
        document_number,
        resources,
        custom_data,
        real_start_date,
        real_end_date,
        created_at
),

final as (
    select
        to_hex(md5(cast(coalesce(cast(e.idtask as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.idhistoric_action as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.idtask_status as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.idtask_type as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.idtask_source as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.iduser as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.idtreatment_by_batch as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.idinstance as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.iddevice as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.idcompany_peer as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.document_number as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.resources as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.custom_data as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.real_start_date as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.real_end_date as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.created_at as string), '_dbt_utils_surrogate_key_null_') as string))) as task_historic_id,

        -- IDs business
        e.idtask as task_id,
        e.idhistoric_action as historic_action_id,
        e.idtask_status as task_status_id,
        e.idtask_type as task_type_id,
        e.idtask_source as task_source_id,
        e.iduser as user_id,
        e.idtreatment_by_batch as treatment_by_batch_id,
        e.idinstance as instance_id,
        e.iddevice as device_id,
        e.idcompany_peer as company_id,

        -- Codes et libellés
        a.code as historic_action_code,
        l.text as historic_action_libelle,
        s.code as task_status_code,
        ty.code as task_type_code,

        -- La tâche au moment de l'action
        e.document_number,
        e.resources as resource_ids,
        e.custom_data,
        e.real_start_date as task_start_date,
        e.real_end_date as task_end_date,

        -- L'ERP n'historise toutes ses actions qu'à partir de cet instant ; avant, des fragments
        e.created_at >= timestamp('2023-08-31 08:49:06', 'Europe/Paris') as is_historique_complet,
        e.nb_lignes_source,

        -- Timestamps
        e.created_at,
        e.extracted_at

    from evenements as e
    left join actions as a
        on e.idhistoric_action = a.idhistoric_action
    left join libelles as l
        on a.idstring = l.idstring
    left join statuts as s
        on e.idtask_status = s.idtask_status
    left join types as ty
        on e.idtask_type = ty.idtask_type
)

select * from final