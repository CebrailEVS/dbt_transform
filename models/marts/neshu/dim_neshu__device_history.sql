{{ config(materialized='table') }}

with snapshot as (
    select * from {{ ref('snap_oracle_neshu__device') }}
),

-- 1 version par machine par JOUR : on garde l'état de fin de journée.
-- Les changements intra-day sont collapsés (le grain aval est quotidien :
-- consumption_date est une date), ce qui rend valid_from unique par machine.
daily as (
    select *
    from snapshot
    qualify row_number() over (
        partition by device_id, date(dbt_valid_from)
        order by dbt_valid_from desc
    ) = 1
),

prepared as (
    select
        dbt_scd_id as device_version_key,
        device_id,

        -- 1ère version plancherée au socle (pas d'historique avant le snapshot)
        case
            when dbt_valid_from = min(dbt_valid_from) over (partition by device_id)
                then date('2000-01-01')
            else date(dbt_valid_from)
        end as valid_from,
        dbt_valid_to,

        device_iddevice as parent_device_id,
        device_type_id,
        company_id,
        company_code,
        company_name,
        location_id,
        device_location,
        device_code,
        device_name,
        device_brand,
        device_gamme,
        device_category,

        -- Backfill des NULL en tête de vie par la 1ère valeur connue (saisie tardive ;
        -- n'agit que sur NULL, un vrai changement X->Y reste intact).
        coalesce(
            device_economic_model,
            first_value(device_economic_model ignore nulls) over (
                partition by device_id
                order by dbt_valid_from
                rows between unbounded preceding and unbounded following
            )
        ) as device_economic_model,

        is_active,
        last_installation_date,
        created_at,
        updated_at
    from daily
)

select
    -- 🔑 Clés
    device_version_key,
    device_id,

    -- 🕒 Validité (SCD Type 2, bornes fermées inclusives ; valid_to via lead)
    valid_from,
    coalesce(
        date_sub(
            lead(valid_from) over (partition by device_id order by valid_from),
            interval 1 day
        ),
        date('9999-12-31')
    ) as valid_to,
    dbt_valid_to is null as is_current,

    -- 🔗 Rattachement (attributs les plus historisés : relocations)
    parent_device_id,
    device_type_id,
    company_id,
    company_code,
    company_name,
    location_id,
    device_location,

    -- 🏷️ Identité machine (quasi stable)
    device_code,
    device_name,
    device_brand,
    device_gamme,
    device_category,

    -- 💶 Modèle économique (backfillé)
    device_economic_model,

    -- 🔧 Statut
    is_active,
    last_installation_date,

    -- 🕒 Dates système (source)
    created_at,
    updated_at

from prepared
