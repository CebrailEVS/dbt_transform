{{ config(
    materialized='table',
    unique_key='intervention_id'
) }}

with nesp_interventions as (

    select
        'NESP' as source,
        'NESPRESSO' as partner,
        CAST(dedup.n_planning as STRING) as intervention_id,
        dedup.n_tech as tech_id,
        dedup.etat_intervention as statut,
        dedup.pickup_date as date_creation,
        dedup.date_heure_debut as date_debut,
        dedup.date_heure_fin as date_fin,
        factu.key_factu,
        dedup.code_postal_site as code_postal,
        factu.prod_factu as prod,
        factu.tarif_factu as montant,
        delai_bonus_bool as bonus_bool,
        factu.tarif_factu + delai_bonus_valeur as montant_avec_bonus,
        delai_heures_debut,
        delai_heures_fin,
        type_delai_debut as delai_tech,
        type_delai_fin as delai_partner

    from {{ ref('int_nesp_tech__interventions_dedup') }} as dedup

    left join {{ ref('int_nesp_tech__facturation_interventions') }} as factu
        on dedup.n_planning = factu.n_planning

    left join {{ ref('int_nesp_tech__delais_interventions') }} as delais
        on dedup.n_planning = delais.n_planning

    where dedup.etat_intervention != 'annulée'

    --{% if is_incremental() %}
--AND dedup.date_heure_fin > (SELECT MAX(date_fin) FROM {{ this }})
--{% endif %}

),

yuman_interventions as (

    select
        'YUMAN' as source,
        inter_yuman.partner_name as partner,
        inter_yuman.workorder_number as intervention_id,
        CAST(inter_yuman.technician_id as STRING) as tech_id,
        inter_yuman.workorder_status as statut,
        TIMESTAMP(inter_yuman.workorder_date_creation) as date_creation,
        inter_yuman.date_started as date_debut,
        inter_yuman.date_done as date_fin,
        inter_yuman.pricing_key_used as key_factu,
        inter_yuman.site_postal_code as code_postal,
        inter_yuman.prod_number as prod,
        inter_yuman.amount as montant,
        false as bonus_bool,
        inter_yuman.amount as montant_avec_bonus,
        0 as delai_heures_debut,
        0 as delai_heures_fin,
        type_delai as delai_tech,
        type_delai as delai_partner

    from `prod_marts.fct_yuman__workorder_delais_neshu` as inter_yuman

    where inter_yuman.billing_validation_status = 'VALIDATED'

    --{% if is_incremental() %}
--AND inter_yuman.date_done > (SELECT MAX(date_fin) FROM {{ this }})
--{% endif %}

),

interventions as (

    select * from nesp_interventions
    union all
    select * from yuman_interventions

)

select
    i.*,

    case
        when
            i.key_factu like '%Aguila%'
            and i.key_factu like '%Montagne%'
            and cp_montagne.montagne = 1
            then 1
        else 0
    end as flag_montagne_prime,

    case
        when STARTS_WITH(i.code_postal, '75') then 1
        else 0
    end as flag_paris_intramuros,

    case
        when
            i.delai_tech in ('J++', 'J+3')
            and i.key_factu like '%Curative%'
            and i.partner = 'NESPRESSO'
            then 1
        else 0
    end as flag_hors_delai_tech,

    tech.user_id as tech_yuman_id,
    tech.nomad_id as tech_nomad_id,
    tech.user_name as tech_nom

from interventions as i

left join {{ ref('ref_yuman__tech_nomad') }} as tech
    on (
        (i.source = 'NESP' and LOWER(tech.nomad_id) = i.tech_id)
        or
        (i.source = 'YUMAN' and CAST(tech.user_id as STRING) = i.tech_id)
    )

left join {{ ref('ref_nesp_tech__cps_montagne_primes') }} as cp_montagne
    on SAFE_CAST(i.code_postal as INT64) = cp_montagne.cp
where date_fin >= '2026-01-01'
