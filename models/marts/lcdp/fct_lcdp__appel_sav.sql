{{ config(materialized='table') }}

with appels as (
    select * from {{ ref('int_oracle_lcdp__appel_sav_tasks') }}
),

ressources as (
    select
        resources_id,
        resources_code,
        resources_name
    from {{ ref('dim_lcdp__resource') }}
)

select
    -- Grain
    date(a.task_start_date, 'Europe/Paris') as appel_date,
    a.task_id,
    a.company_id,
    a.device_id,
    a.resources_id,

    -- Attributs d'affichage
    a.document_number as appel_numero,
    {{ utc_vers_paris_affichage('a.task_start_date') }} as appel_start_at,
    {{ utc_vers_paris_affichage('a.task_end_date') }} as appel_end_at,
    a.company_code,
    a.company_name,
    a.device_code,
    a.device_name,
    r.resources_code,
    r.resources_name,

    -- Catégorisation saisie dans l'ERP
    a.appel_categorie_code,
    a.appel_categorie_label,
    a.appel_sous_categorie_code,
    a.appel_sous_categorie_label,
    a.appel_detail_label,
    a.task_status_code,

    -- Commentaires
    a.comments_self,
    a.comments_peer,

    -- Métadonnées
    {{ utc_vers_paris_affichage('a.created_at') }} as created_at,
    {{ utc_vers_paris_affichage('a.updated_at') }} as updated_at,
    a.extracted_at

from appels as a
left join ressources as r
    on a.resources_id = r.resources_id
