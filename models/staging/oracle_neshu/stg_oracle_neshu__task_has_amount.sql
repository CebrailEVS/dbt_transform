{{
    config(
        materialized='incremental',
        unique_key=['idtask', 'tax_rate', 'idtax', 'idtax_region'],
        partition_by={'field': 'updated_at', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        cluster_by=['idtask', 'idtax', 'idtax_region'],
        description='Montants et taxes par tâche depuis task_has_amount. Porte le coût des produits chargés du P&L client.'
    )
}}

-- ⚠️ Deux particularités par rapport aux autres staging oracle_neshu.
--
-- 1. La table n'a NI creation_date NI modification_date. Le pipeline dlt la
--    réplique via un curseur emprunté à son parent `task`, qui atterrit dans
--    `_parent_modification_date`. C'est donc lui qui alimente `updated_at`, et
--    `created_at` n'existe pas ici.
--
-- 2. La clé fait QUATRE colonnes : une même tâche porte une ligne par taux et
--    par région de taxe. `tax_rate` en fait partie, elle reste donc en NUMERIC
--    exact — la caster en float64 comme les autres montants ferait s'écraser
--    des lignes distinctes au merge, sans erreur.

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_task_has_amount') }}
),

cleaned_data as (
    select
        -- IDs (clé de merge composite)
        cast(idtask as int64) as idtask,
        cast(idtax as int64) as idtax,
        cast(idtax_region as int64) as idtax_region,

        -- Composante de la clé : précision exacte obligatoire (cf. en-tête)
        cast(tax_rate as numeric) as tax_rate,

        -- Colonnes texte
        tax_code,
        tax_name,

        -- Colonnes numériques
        cast(tax_amount as float64) as tax_amount,
        cast(amount_without_tax as float64) as amount_without_tax,
        cast(percentage as float64) as percentage,

        -- Timestamps harmonisés (standard DBT)
        timestamp(_parent_modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
),

-- ⚠️ Sécurité de jointure pour éviter les orphelins
filtered_data as (
    select c.*
    from cleaned_data as c
    inner join {{ ref('stg_oracle_neshu__task') }} as t
        on c.idtask = t.idtask
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
