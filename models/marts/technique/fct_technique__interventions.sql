{{
    config(
        materialized='table',
        partition_by={
            'field': 'date_debut',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['partenaire', 'src_inter']
    )
}}

-- CTE 1 : Interventions Nespresso enrichies (facturation + délais)
with nesp_interventions as (
    select
        'NESP' as src_inter,
        'NESPRESSO' as partenaire,
        cast(dedup.n_planning as string) as intervention_id,
        dedup.n_tech as tech_id,
        factu.categorie_machine,
        factu.machine_clean,
        dedup.etat_intervention as intervention_statut,
        case
            when dedup.etat_intervention in ('terminée signée', 'signature différée') then 'VALIDATED'
            when dedup.etat_intervention in ('mise en échec') then 'NOT VALIDATED'
            else 'NOT DEFINED'
        end as statut_facturation,
        dedup.pickup_date as date_creation,
        dedup.date_heure_debut as date_debut,
        dedup.date_heure_fin as date_fin,
        factu.key_factu,
        dedup.code_postal_site as code_postal,
        dedup.consignes,
        dedup.observations as commentaire_tech,
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
        cast(inter_yuman.technician_id as string) as tech_id,
        inter_yuman.machine_clean as categorie_machine,
        inter_yuman.machine_raw as machine_clean,
        inter_yuman.workorder_status as intervention_statut,
        inter_yuman.billing_validation_status as statut_facturation,
        timestamp(inter_yuman.workorder_date_creation) as date_creation,
        inter_yuman.date_started as date_debut,
        inter_yuman.date_done as date_fin,
        inter_yuman.pricing_key_used as key_factu,
        inter_yuman.site_postal_code as code_postal,
        inter_yuman.demand_description as consignes,
        inter_yuman.workorder_report as commentaire_tech,
        inter_yuman.prod_number as prod,
        inter_yuman.amount as montant,
        false as bonus_bool,
        inter_yuman.amount as montant_avec_bonus,
        0 as delai_heures_debut,
        0 as delai_heures_fin,
        type_delai as delai_tech,
        type_delai as delai_partenaire
    from {{ ref('fct_yuman__workorder_delais_neshu') }} as inter_yuman
    where inter_yuman.demand_status = 'Accepted'
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
        concat(i.intervention_id, '_', i.partenaire) as key_inter,
        i.src_inter,
        i.partenaire,
        i.intervention_id,
        i.intervention_statut,
        i.statut_facturation,
        i.categorie_machine,
        i.machine_clean,
        i.tech_id,
        i.date_creation,
        i.date_debut,
        i.date_fin,
        i.key_factu,
        i.code_postal,
        i.consignes,
        i.commentaire_tech,
        i.prod,
        i.montant,
        i.bonus_bool,
        i.montant_avec_bonus,
        i.delai_heures_debut,
        i.delai_heures_fin,
        i.delai_tech,
        i.delai_partenaire,

        -- Calcul durée en minutes
        timestamp_diff(i.date_fin, i.date_debut, minute) as duree_inter_minutes,

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
            when starts_with(i.code_postal, '75') then 1
            else 0
        end as flag_paris_intramuros,

        -- Flag hors délai technicien
        case
            when
                i.delai_tech in ('J++', 'J+3')
                and lower(i.key_factu) like '%curative%'
                --and i.partenaire = 'NESPRESSO'
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
            (i.src_inter = 'NESP' and lower(tech.nomad_id) = i.tech_id)
            or (i.src_inter = 'YUMAN' and cast(tech.user_id as string) = i.tech_id)
        )
    left join {{ ref('ref_nesp_tech__cps_montagne_primes') }} as cp_montagne
        on safe_cast(i.code_postal as int64) = cp_montagne.cp
)

select
    key_inter,
    src_inter,
    partenaire,
    intervention_id,
    intervention_statut,
    statut_facturation,
    coalesce(categorie_machine, 'UNDEFINED') as categorie_machine,
    coalesce(machine_clean, 'UNDEFINED') as machine_clean,
    tech_id,
    date_creation,
    date_debut,
    date_fin,
    duree_inter_minutes,
    key_factu,
    code_postal,
    consignes,
    commentaire_tech,
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
