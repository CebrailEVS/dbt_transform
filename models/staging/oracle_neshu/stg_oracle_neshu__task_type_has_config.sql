{{
    config(
        materialized='table',
        description='Paramétrage par type de tâche depuis task_type_has_config. Porte le coefficient de signe du chiffre d''affaires.'
    )
}}

-- `value` est un VARCHAR côté ERP et le reste ici : le raw atterrit fidèle à la
-- source. La ligne qui compte est idconfig = 'coefficient', qui vaut 1 sur le
-- type 102 FACT CLIENT et -1 sur le 106 AVOIR — c'est la seule source de la
-- règle qui fait qu'un avoir se retranche du CA au lieu de s'y ajouter. Le cast
-- se fait à l'usage, en intermediate.

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_task_type_has_config') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtask_type as int64) as idtask_type,
        cast(idinstance as int64) as idinstance,
        cast(iduser as int64) as iduser,

        -- Colonnes texte
        idconfig,
        value,
        comments,

        -- Booléen
        cast(cast(edit_customer as int64) as boolean) as is_edit_customer,

        -- Timestamps harmonisés
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
