

-- Compilation des 6 flux de retraitement de l'app Suivi Tech en un modèle unique,
-- une ligne par (key_inter, type_retraitement). RW et events sont exclus
-- (RW = clawback de prime, cf. §2.3 note_context ; events = référentiel technicien).
-- Chaque flux apporte ses colonnes manager propres ; NULL pour celles qui ne le
-- concernent pas. L'arbitrage entre types touchant une même intervention est
-- volontairement repoussé au mart Facturation retraitée (modèle 1).
--
-- Identité : key_inter = concat(src_inter, '_', intervention_id) reconstruit la PK
-- de fct_technique__intervention (workorder_id/n_planning chevauchent → src_inter
-- requis). src_inter/numero_pu proviennent du NDJSON (contrat identité 2026-07) ;
-- ils sont NULL tant que l'app ne les émet pas — key_inter est alors NULL et la
-- jointure au fait est vide, sans casser le build (bascule sûre).

with unioned as (

    select
        intervention_id,
        src_inter,
        numero_pu,
        'astreinte' as type_retraitement,
        cast(null as string) as a_facturer,
        tech_id_reel,
        tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        cast(null as string) as type_modif,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_astreinte`

    union all

    select
        intervention_id,
        src_inter,
        numero_pu,
        'mee' as type_retraitement,
        a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        cast(null as string) as type_modif,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_mee`

    union all

    select
        intervention_id,
        src_inter,
        numero_pu,
        'curative' as type_retraitement,
        cast(null as string) as a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        delai_tech_force,
        delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        cast(null as string) as type_modif,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_curative`

    union all

    select
        intervention_id,
        src_inter,
        numero_pu,
        'aguila' as type_retraitement,
        cast(null as string) as a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        convertir_code_5,
        cast(null as string) as type_modif,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_aguila`

    union all

    select
        intervention_id,
        src_inter,
        numero_pu,
        'pause' as type_retraitement,
        cast(null as string) as a_facturer,
        cast(null as string) as tech_id_reel,
        cast(null as int64) as tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        doubler_prime,
        cast(null as string) as convertir_code_5,
        cast(null as string) as type_modif,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_pause`

    union all

    select
        intervention_id,
        src_inter,
        numero_pu,
        'modif_intervention' as type_retraitement,
        a_facturer,
        tech_id_reel,
        tech_yuman_id_reel,
        cast(null as string) as delai_tech_force,
        cast(null as string) as delai_partenaire_force,
        cast(null as string) as doubler_prime,
        cast(null as string) as convertir_code_5,
        type_modif,
        commentaire,
        periode,
        periode_date,
        source_file,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_modif_intervention`

),

-- Dedup GLOBAL sur (src_inter, intervention_id, type_retraitement) = (key_inter,
-- type) : au-delà de la periode, car modif_intervention n'a pas de restriction de
-- mois. On garde la ligne dont (periode, extracted_at) est la plus récente. La
-- clé inclut src_inter (partition tolérante à src_inter NULL pendant la bascule).
deduped as (
    select * from unioned
    qualify row_number() over (
        partition by src_inter, intervention_id, type_retraitement
        order by periode_date desc, extracted_at desc
    ) = 1
)

select
    concat(src_inter, '_', intervention_id) as key_inter,
    intervention_id,
    src_inter,
    numero_pu,
    type_retraitement,
    a_facturer,
    tech_id_reel,
    tech_yuman_id_reel,
    delai_tech_force,
    delai_partenaire_force,
    doubler_prime,
    convertir_code_5,
    type_modif,
    commentaire,
    periode,
    periode_date,
    source_file,
    extracted_at
from deduped