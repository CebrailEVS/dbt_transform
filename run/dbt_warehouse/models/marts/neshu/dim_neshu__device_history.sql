
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_neshu__device_history`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension machine **versionn\u00e9e (SCD Type 2)** : l'\u00e9tat historique de chaque machine (rattachement client, localisation, mod\u00e8le \u00e9conomique, statut...) \u00e0 chaque p\u00e9riode de validit\u00e9. Sert aux jointures \u00ab \u00e0 la date \u00bb (point-in-time) : retrouver l'\u00e9tat d'une machine au moment d'une consommation, plut\u00f4t que son \u00e9tat courant.\n[COMMENT CONSTRUITE] Issu du snapshot snap_oracle_neshu__device (SCD2 g\u00e9r\u00e9 par dbt/Cloud Workflows). Les bornes dbt_valid_from/dbt_valid_to sont expos\u00e9es en valid_from/valid_to : bornes semi-ouvertes [valid_from, valid_to) en timestamp (valid_to = dbt_valid_to, exclusif ; 9999-12-31 si version ouverte). La 1\u00e8re version est plancher\u00e9e au 2000-01-01 (pas d'historique avant le d\u00e9marrage du snapshot). device_economic_model : backfill des NULL en t\u00eate de vie par la 1\u00e8re valeur connue de la machine (rattrapage de saisie tardive \u2014 n'agit que sur NULL, un vrai changement X\u2192Y reste intact). Aucun filtre marque/type : dimension g\u00e9n\u00e9rale, c'est au fait consommateur de filtrer son p\u00e9rim\u00e8tre.\n[GRAIN] 1 ligne par machine \u00d7 p\u00e9riode de validit\u00e9 (device_id \u00d7 valid_from). PK = device_version_key.\n[NOTES] L'historisation porte surtout sur le rattachement (client/localisation/date d'installation) ; le mod\u00e8le \u00e9conomique change rarement (~43 machines). Horizon limit\u00e9 au d\u00e9marrage du snapshot (nov. 2025) ; avant = pas d'historique (1\u00e8re version plancher\u00e9e 2000-01-01). Jointure point-in-time : `event_ts >= valid_from and event_ts < valid_to` ; une date (= minuit) matche aussi une seule p\u00e9riode (\u00e9tat d\u00e9but de journ\u00e9e). is_current = TRUE pour la version courante. Pour un simple \u00e9tat courant, pr\u00e9f\u00e9rer dim_neshu__device.\n"""
    )
    as (
      

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
    );
  