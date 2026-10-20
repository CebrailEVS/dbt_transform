
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic`
      
    partition by timestamp_trunc(created_at, month)
    cluster by idtask_type, idhistoric_action, idtask

    
    OPTIONS(
      description="""Historique des actions sur les t\u00e2ches, nettoy\u00e9 depuis la base Oracle : une ligne par action (cr\u00e9ation, changement de statut, de date, de produits, synchro EasyMobile, traitement par lot, suppression), avec la t\u00e2che telle qu'elle est apr\u00e8s l'action. Ni cl\u00e9 ni d\u00e9duplication : la source \u00e9crit des doublons exacts l\u00e9gitimes, regroup\u00e9s en intermediate. Ajout seul : une ligne que l'ERP purge reste ici."""
    )
    as (
      

-- Pas de unique_key : la source écrit des doublons exacts légitimes, qu'un merge sur une clé
-- écraserait. Le raw ne fait qu'ajouter des lignes, le staging aussi.

with chargements_complets as (
    -- Un run dlt qui échoue après le chargement de la table y laisse des lignes que sa relance
    -- réajoute : seules celles des chargements complets sont lues.
    select load_id
    from `evs-datastack-prod`.`prod_raw`.`_dlt_loads`
    where status = 0
),

source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_task_historic`
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
        timestamp(datetime(real_start_date), 'Europe/Paris') as real_start_date,
        timestamp(datetime(real_end_date), 'Europe/Paris') as real_end_date,

        -- Timestamps harmonisés : la ligne naît à l'action et ne change plus
        timestamp(datetime(modification_date), 'Europe/Paris') as created_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data

    );
  