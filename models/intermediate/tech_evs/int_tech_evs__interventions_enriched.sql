{{ config(materialized='table',unique_key='intervention_id') }}

-- CTE 1 : Interventions Nespresso enrichies (facturation + délais)
with nesp_interventions as (
    select
        'NESP' as src_inter,
        'NESPRESSO' as partenaire,
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
        delais.delai_bonus_bool as bonus_bool,
        factu.tarif_factu + delais.delai_bonus_valeur as montant_avec_bonus,
        delais.delai_heures_debut,
        delais.delai_heures_fin,
        delais.type_delai_debut as delai_tech,
        delais.type_delai_fin as delai_partenaire
    from {{ ref('int_nesp_tech__interventions_dedup') }} as dedup
    left join {{ ref('int_nesp_tech__facturation_interventions') }} as factu
        on dedup.n_planning = factu.n_planning
    left join {{ ref('int_nesp_tech__delais_interventions') }} as delais
        on dedup.n_planning = delais.n_planning
    where dedup.etat_intervention != 'annulée'
),

-- CTE 2 : Interventions Yuman normalisées au même format
yuman_interventions as (
    select
        'YUMAN' as src_inter,
        inter_yuman.partner_name as partenaire,
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
        type_delai as delai_partenaire
    from `prod_marts.fct_yuman__workorder_delais_neshu` as inter_yuman
    where inter_yuman.billing_validation_status = 'VALIDATED'
),

-- CTE 3 : Union des deux sources homogénéisées
interventions as (
    select * from nesp_interventions
    union all
    select * from yuman_interventions
),

-- CTE 4 : Enrichissement métier (durée + flags + mapping techniciens)
interventions_enrichies as (
    select
        i.src_inter,
        i.partenaire,
        i.intervention_id,
        i.tech_id,
        i.statut,
        i.date_creation,
        i.date_debut,
        i.date_fin,
        i.key_factu,
        i.code_postal,
        i.prod,
        i.montant,
        i.bonus_bool,
        i.montant_avec_bonus,
        i.delai_heures_debut,
        i.delai_heures_fin,
        i.delai_tech,
        i.delai_partenaire,

        -- Calcul durée en minutes
        TIMESTAMP_DIFF(i.date_fin, i.date_debut, minute) as duree_inter_minutes,

        -- Flag montagne (prime spécifique)
        case
            when
                i.key_factu like '%Aguila%'
                and i.key_factu like '%Montagne%'
                and cp_montagne.montagne = 1
                then 1
            else 0
        end as flag_montagne_prime,

        -- Flag Paris intramuros
        case
            when STARTS_WITH(i.code_postal, '75') then 1
            else 0
        end as flag_paris_intramuros,

        -- Flag hors délai technicien
        case
            when
                i.delai_tech in ('J++', 'J+3')
                and i.key_factu like '%Curative%'
                and i.partenaire = 'NESPRESSO'
                then 1
            else 0
        end as flag_hors_delai_tech,

        -- Mapping technicien
        tech.user_id as tech_yuman_id,
        tech.nomad_id as tech_nomad_id,
        tech.user_name as tech_nom

    from interventions as i
    left join {{ ref('ref_yuman__tech_nomad') }} as tech
        on (
            (i.src_inter = 'NESP' and LOWER(tech.nomad_id) = i.tech_id)
            or (i.src_inter = 'YUMAN' and CAST(tech.user_id as STRING) = i.tech_id)
        )
    left join {{ ref('ref_nesp_tech__cps_montagne_primes') }} as cp_montagne
        on SAFE_CAST(i.code_postal as INT64) = cp_montagne.cp
)

-- SELECT final (filtrage)
-- SELECT final (filtrage + colonnes explicites)
select
    src_inter,
    partenaire,
    intervention_id,
    tech_id,
    statut,
    date_creation,
    date_debut,
    date_fin,
    duree_inter_minutes,
    key_factu,
    code_postal,
    prod,
    montant,
    bonus_bool,
    montant_avec_bonus,
    delai_heures_debut,
    delai_heures_fin,
    delai_tech,
    delai_partenaire,
    flag_montagne_prime,
    flag_paris_intramuros,
    flag_hors_delai_tech,
    tech_yuman_id,
    tech_nomad_id,
    tech_nom
from interventions_enrichies
where date_fin >= '2026-01-01'
