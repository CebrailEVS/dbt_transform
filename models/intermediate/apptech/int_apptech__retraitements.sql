{{ config(materialized='table') }}

-- Compilation des 6 flux de retraitement de l'app Suivi Tech en un modèle unique,
-- une ligne par (intervention_id, type_retraitement). RW et events sont exclus
-- (RW = clawback de prime, cf. §2.3 note_context ; events = référentiel technicien).
-- Chaque flux apporte ses colonnes manager propres ; NULL pour celles qui ne le
-- concernent pas. L'arbitrage entre types touchant une même intervention est
-- volontairement repoussé au mart Facturation retraitée (modèle 1).

with unioned as (

    select
        intervention_id,
        'astreinte' as type_retraitement,
        cast(null as string) as a_facturer,
        tech_id_reel,
        tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from {{ ref('stg_apptech__suivi_tech_astreinte') }}

    union all

    select
        intervention_id,
        'mee' as type_retraitement,
        a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from {{ ref('stg_apptech__suivi_tech_mee') }}

    union all

    select
        intervention_id,
        'curative' as type_retraitement,
        cast(null as string) as a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        delai_tech_force,
        delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from {{ ref('stg_apptech__suivi_tech_curative') }}

    union all

    select
        intervention_id,
        'aguila' as type_retraitement,
        cast(null as string) as a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        convertir_code_5,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from {{ ref('stg_apptech__suivi_tech_aguila') }}

    union all

    select
        intervention_id,
        'pause' as type_retraitement,
        cast(null as string) as a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        doubler_prime,
        cast(null as string) as convertir_code_5,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from {{ ref('stg_apptech__suivi_tech_pause') }}

    union all

    select
        intervention_id,
        'modif_intervention' as type_retraitement,
        a_facturer,
        tech_id_reel,
        tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from {{ ref('stg_apptech__suivi_tech_modif_intervention') }}

)

-- Dedup GLOBAL sur (intervention_id, type_retraitement) : au-delà de la periode,
-- car modif_intervention n'a pas de restriction de mois (une même intervention
-- peut être re-modifiée dans une periode ultérieure). On garde la ligne dont
-- (periode, extracted_at) est la plus récente.
select * from unioned
qualify row_number() over (
    partition by intervention_id, type_retraitement
    order by periode_date desc, extracted_at desc
) = 1
