{{
    config(
        materialized='incremental',
        unique_key=['idlabel', 'idtask_has_product'],
        partition_by={'field': 'updated_at', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        cluster_by=['idtask_has_product'],
        description='Étiquettes posées sur une LIGNE de produit depuis label_has_thp. Porte notamment le mode de paiement des ventes télémétrie.'
    )
}}

-- ⚠️ Comme task_has_amount, la table n'a aucune colonne de modification : dlt la
-- réplique via un curseur emprunté, mais à `task_has_product` cette fois et non
-- à `task`. C'est la seule jonction du périmètre dans ce cas. Le curseur atterrit
-- dans `_parent_modification_date` et alimente `updated_at` ; `created_at`
-- n'existe pas.
--
-- Ne pas confondre avec `label_has_product`, qui étiquette un PRODUIT du
-- catalogue. Ici c'est la ligne de produit d'une tâche.

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_label_has_thp') }}
),

cleaned_data as (
    select
        -- IDs (clé de merge composite)
        cast(idlabel as int64) as idlabel,
        cast(idtask_has_product as int64) as idtask_has_product,

        -- Timestamps harmonisés (standard DBT)
        timestamp(_parent_modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
),

-- ⚠️ Sécurité de jointure pour éviter les orphelins
filtered_data as (
    select c.*
    from cleaned_data as c
    inner join {{ ref('stg_oracle_neshu__task_has_product') }} as thp
        on c.idtask_has_product = thp.idtask_has_product
)

select * from filtered_data
{% if is_incremental() %}
    where
        (
            updated_at > (
                select max(t.updated_at)
                from {{ this }} as t
            )
            or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
        )
{% endif %}
