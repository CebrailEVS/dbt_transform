

-- Dépend du snapshot SCD2 snap_oracle_neshu__device. Les snapshots sont un
-- artefact de PROD (historique accumulé) et ne sont pas reconstruits en dev.
-- Le build dev tourne donc avec --defer : ref('snap_...') résout vers la
-- version prod (evs-datastack-prod.snapshots). Cf. CI pr-check.

with snapshot as (
    select * from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__device`
)

select
    -- 🔑 Clés
    dbt_scd_id as device_version_key,   -- PK : 1 ligne = 1 version de machine
    device_id,                          -- clé naturelle durable (stable dans le temps)

    -- 🕒 Validité (SCD Type 2, bornes semi-ouvertes [valid_from, valid_to) en timestamp).
    -- 1ère version plancherée au socle : pas d'historique avant le démarrage du snapshot,
    -- on considère le 1er état connu valide « depuis toujours ».
    case
        when dbt_valid_from = min(dbt_valid_from) over (partition by device_id)
            then timestamp('2000-01-01')
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31')) as valid_to,   -- exclusif ; 9999 si courante
    dbt_valid_to is null as is_current,

    -- 🔗 Rattachement (attributs les plus historisés : relocations)
    device_iddevice as parent_device_id,
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

    -- 💶 Modèle économique — backfill des NULL en tête de vie par la 1ère valeur connue
    -- (rattrapage de saisie tardive : la machine était déjà X, juste pas saisi ;
    -- n'agit que sur NULL, un vrai changement X->Y reste intact).
    coalesce(
        device_economic_model,
        first_value(device_economic_model ignore nulls) over (
            partition by device_id
            order by dbt_valid_from
            rows between unbounded preceding and unbounded following
        )
    ) as device_economic_model,

    -- 🔧 Statut
    is_active,
    last_installation_date,

    -- 🕒 Dates système (source)
    created_at,
    updated_at

from snapshot