{{ config(
    materialized='table',
    partition_by={
        'field': 'task_start_date',
        'data_type': 'timestamp',
        'granularity': 'month'
    },
    cluster_by=['roadman_code', 'company_name']
) }}

-- Passages des approvisionneurs Neshu validés (heures de travail, onglet Clients) :
-- l'app interroge cette table à la demande au lieu de garder les passages en
-- mémoire. task_start_date est en heure locale française dans la source (étiquette
-- UTC sans conversion) : repris tel quel, l'app le lit comme une heure locale.
select
    task_id,
    task_start_date,
    task_end_date,
    roadman_code,
    company_id,
    company_code,
    company_name,
    company_info,
    device_id,
    device_code,
    device_name,
    device_info,
    gea_code,
    task_location_info,
    task_status_code,
    is_done,
    is_planned,
    is_anomaly,
    passage_duration_min,
    passage_duration_hours
from {{ ref('int_oracle_neshu__appro_tasks_enriched') }}
-- Statuts retenus par l'app (config.STATUS_VALIDES) ; un passage sans
-- approvisionneur ou sans date n'est jamais affiché
where
    task_status_code in ('FAIT', 'VALIDE')
    and roadman_code is not null
    and task_start_date is not null
