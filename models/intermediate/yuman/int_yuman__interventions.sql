{{ config(
    materialized='table',
    schema='intermediate',
    alias='int_yuman__interventions',
    partition_by={"field": "date_done", "data_type": "timestamp"},
    cluster_by=['client_id', 'site_id', 'material_id']
) }}

-- Modèle intermédiaire consolidé des interventions Yuman.
-- Fusionne ici, en CTE, ce qui était auparavant éclaté en deux faits chaînés
-- (fct_technique__workorder_pricing puis fct_neshu__workorder_delai) afin de
-- supprimer les dépendances fait→fait. Source de vérité unique pour :
--   1. la normalisation type/machine/métropole + extraction code postal
--   2. la tarification automatique (récurrence → type tarif → montant)
--   3. le délai de traitement en jours ouvrés et son type (J+0,5 … J++)
--   4. la qualification métier de l'intervention (intervention_state + flags)
-- Périmètre : exclut les bons de travail sans demande rattachée (workorders "secs"),
-- dépourvus de référentiel client/partenaire fiable (cf. filtre demand_id is not null).

-- CTE 1 : base enrichie + dérivations brutes (type, machine, code postal, à facturer)
with base_workorders as (
    select
        demand_id,
        workorder_id,
        material_id,
        site_id,
        client_id,
        technician_id,
        manager_id,
        demand_description,
        demand_status,
        demand_created_at,
        demand_updated_at,
        demand_category_name,
        workorder_number,
        workorder_category,
        workorder_status,
        workorder_technician_name,
        workorder_date_creation,
        workorder_report,
        workorder_motif_non_intervention,
        workorder_detail_non_intervention,
        workorder_raison_mise_en_pause,
        workorder_explication_mise_en_pause,
        is_workorder_paused,
        is_workorder_currently_paused,
        is_workorder_not_done,
        date_planned,
        date_started,
        date_done,
        partner_name,
        client_code,
        client_name,
        client_category,
        client_is_active,
        site_code,
        site_name,
        site_address,
        site_postal_code,
        material_serial_number,
        technician_equipe,

        -- Etat metier canonique (defini une seule fois dans le modele enrichi amont)
        intervention_state,
        is_realized,
        has_workorder,

        lower(coalesce(
            workorder_category,
            case
                when workorder_type = 'Reactive' then 'curatif'
                when workorder_type = 'Preventive' then 'préventif'
                when workorder_type = 'Installation' then 'installation'
                else workorder_type
            end
        )) as workorder_type_raw,

        lower(trim(case
            when material_category is not null then material_category
            when lower(client_name) like '%generique%' then concat(partner_name, '_GENERIQUE')
            when partner_name = 'AUUM' then 'MGZ'
            when partner_name = 'TWYD' then 'FONTAINE TWYD'
            when partner_name = 'EXPRESSO' then 'MILANO'
            when partner_name = 'NESHU' then 'MILANO'
            when partner_name = 'BRITA' then 'viv t 85 c2-tg-i-cu ce'
            when partner_name = 'DAAN' then 'lave-vaisselle'
            when partner_name = 'NU' then 'frigo nu'
        end)) as machine_raw,

        case
            when site_postal_code is null or site_postal_code = '00000'
                then (
                    select code_postal
                    from unnest(regexp_extract_all(demand_description, r'\b\d{5}\b')) as code_postal
                    where
                        not regexp_contains(
                            left(demand_description, strpos(demand_description, code_postal) - 1),
                            r'(?i)N°\s*$|interventions?\s*$'
                        )
                    limit 1  -- noqa: AM09
                )
            else regexp_extract(site_address, r'\b(\d{5})\b')
        end as postal_code_site,

        (
            workorder_status = 'Closed'
            and workorder_motif_non_intervention is null
            and workorder_detail_non_intervention is null
        ) as a_facturer

    from {{ ref('int_yuman__demands_workorders_enriched') }}
    -- Exclusion des bons de travail "secs" (orphelins, sans demande rattachée) : ils
    -- décrochent du référentiel client/partenaire (partner_name, client_id… NULL) et
    -- fausseraient la clé d'intervention aval (key_inter) + la tarification (~65 lignes).
    -- Flag canonique défini une seule fois en amont (cf. is_orphan_workorder).
    where not is_orphan_workorder
),

-- CTE de référence : nettoyage type d'intervention, machine, métropole, tarification
ref_type_inter as (
    select
        lower(type_intervention_brut) as workorder_type_raw,
        lower(type_inter) as workorder_type_clean
    from {{ ref('ref_yuman__type_inter_clean') }}
),

ref_machine as (
    select
        machine_raw,
        machine_clean
    from (
        select
            lower(trim(machine_brut)) as machine_raw,
            lower(trim(machine)) as machine_clean,
            row_number() over (partition by lower(trim(machine_brut)) order by machine) as rn
        from {{ ref('ref_yuman__machine_clean') }}
    )
    where rn = 1
),

ref_cp_metropole as (
    select
        code_postal,
        metropole
    from {{ ref('ref_yuman__cp_metropole') }}
),

ref_dpt_metropole as (
    select
        departement,
        metropole
    from {{ ref('ref_yuman__dpt_metropole') }}
),

ref_tarification as (
    select
        lower(concat(
            type_inter, '_',
            machine, '_',
            marque, '_',
            type_tarif, '_',
            cast(metropole as string)
        )) as key_tarif,
        montant,
        prod,
        valid_from,
        coalesce(valid_to, date('9999-12-31')) as valid_to
    from {{ ref('ref_yuman__tarification_clean') }}
),

-- CTE 2 : normalisation type/machine + détection métropole
workorders_enriched as (
    select
        w.*,
        coalesce(ti.workorder_type_clean, w.workorder_type_raw) as workorder_type_clean,
        coalesce(m.machine_clean, w.machine_raw) as machine_clean,
        coalesce(cp.metropole, dp.metropole) as metropole_city,
        case
            when w.postal_code_site is null then 1
            when cp.code_postal is not null then 1
            when dp.departement is not null then 1
            else 0
        end as metropole
    from base_workorders as w
    left join ref_type_inter as ti
        on w.workorder_type_raw = ti.workorder_type_raw
    left join ref_machine as m
        on w.machine_raw = m.machine_raw
    left join ref_cp_metropole as cp
        on w.postal_code_site = cp.code_postal
    left join ref_dpt_metropole as dp
        on left(w.postal_code_site, 2) = dp.departement
),

-- CTE 3 : récurrence (nb d'interventions facturables même site / même jour)
workorders_dedup as (
    select
        *,
        case
            when postal_code_site is null then 1
            else count(
                case when a_facturer then site_id end
            ) over (partition by site_id, date(date_done))
        end as reccurence
    from workorders_enriched
),

-- CTE 4 : type de tarif selon paliers de récurrence par partenaire
workorders_with_tarif as (
    select
        *,
        case
            when postal_code_site is null then 'Tarif normal'
            when partner_name in ('FONTAINCO', 'TWYD', 'NESHU', 'NU', 'DAANTECH', 'EXPRESSO', 'DAAN')
                then
                    case
                        when reccurence < 5 then 'Tarif normal'
                        when reccurence between 5 and 20 then 'Remise niv1'
                        else 'Remise niv2'
                    end
            when partner_name in ('BRITA', 'AUUM')
                then
                    case
                        when reccurence < 2 then 'Tarif normal'
                        when reccurence between 2 and 5 then 'Remise niv1'
                        else 'Remise niv2'
                    end
            else 'Tarif normal'
        end as type_tarif
    from workorders_dedup
),

-- CTE 5 : application du tarif (jointure table de référence, dédup grain demand/workorder)
priced as (
    select
        w.*,
        t.montant,
        t.prod,
        lower(concat(
            w.workorder_type_clean, '_',
            w.machine_clean, '_',
            w.partner_name, '_',
            w.type_tarif, '_',
            cast(w.metropole as string)
        )) as key_tarif_used
    from workorders_with_tarif as w
    left join ref_tarification as t
        on
            lower(concat(
                w.workorder_type_clean, '_',
                w.machine_clean, '_',
                w.partner_name, '_',
                w.type_tarif, '_',
                cast(w.metropole as string)
            )) = t.key_tarif
            and date(w.date_done) between t.valid_from and t.valid_to
    qualify
        row_number() over (
            partition by w.demand_id, w.workorder_id
            order by t.valid_from desc nulls last
        ) = 1
),

-- CTE 6 : calcul du délai en jours ouvrés (date de réf décalée si création > 16h)
adjusted_dates as (
    select
        workorder_id as wo_id,
        coalesce(timestamp(workorder_date_creation), demand_created_at) as date_creation_initial,
        date_done as date_fin,
        case
            when extract(time from coalesce(timestamp(workorder_date_creation), demand_created_at)) > '16:00:00'
                then timestamp(date(coalesce(timestamp(workorder_date_creation), demand_created_at)) + 1)
            else coalesce(timestamp(workorder_date_creation), demand_created_at)
        end as date_creation_ref
    from priced
),

dates_range as (
    select
        ad.wo_id,
        ad.date_creation_initial,
        ad.date_fin,
        ad.date_creation_ref,
        date_jour
    from adjusted_dates as ad,
        unnest(
            generate_date_array(
                date(ad.date_creation_ref),
                date(ad.date_fin),
                interval 1 day
            )
        ) as date_jour
),

jours_feries as (
    select date_ferie
    from {{ ref('ref_general__feries_metropole') }}
),

jours_ouvrables as (
    select
        dr.wo_id,
        dr.date_creation_ref,
        dr.date_fin,
        dr.date_jour,
        jf.date_ferie
    from dates_range as dr
    left join jours_feries as jf
        on dr.date_jour = jf.date_ferie
    where
        extract(dayofweek from dr.date_jour) not in (1, 7)
        and jf.date_ferie is null
),

delai_calcul as (
    select
        wo_id,
        date_creation_ref,
        date_fin,
        count(date_jour) - 1 as delai_jours_ouvres
    from jours_ouvrables
    group by wo_id, date_creation_ref, date_fin
),

famille_machine as (
    select
        machine_brut,
        famille_neshu
    from {{ ref('ref_yuman__machine_clean') }}
),

-- CTE 7 : assemblage final + qualification métier (état + délai + flags)
final_table as (
    select
        p.*,
        dc.date_creation_ref,
        dc.delai_jours_ouvres,
        fm.famille_neshu,
        case
            when dc.delai_jours_ouvres = 0
                then 'J+0,5'
            when
                dc.delai_jours_ouvres = 1
                and extract(time from dc.date_creation_ref) > '12:00:00'
                and extract(time from dc.date_fin) < '12:00:00'
                then 'J+0,5'
            when dc.delai_jours_ouvres = 1
                then 'J+1'
            when dc.delai_jours_ouvres = 2
                then 'J+2'
            when dc.delai_jours_ouvres > 2
                then 'J++'
            else 'ERREUR'
        end as type_delai
    -- Etat metier canonique (intervention_state, is_realized, has_workorder) deja
    -- present via p.* : source de verite unique dans le modele enrichi amont.
    from priced as p
    left join delai_calcul as dc
        on p.workorder_id = dc.wo_id
    left join famille_machine as fm
        on p.machine_raw = fm.machine_brut
)

select
    -- Clés / grain
    demand_id,
    workorder_id,
    workorder_number,

    -- Dimensions rattachées (FK)
    material_id,
    site_id,
    client_id,
    technician_id,
    manager_id,

    -- Etat métier
    intervention_state,
    workorder_status,
    demand_status,
    is_realized,
    has_workorder,
    is_workorder_paused,
    is_workorder_currently_paused,
    is_workorder_not_done,

    -- Attributs intervention
    demand_category_name,
    workorder_category,
    workorder_type_raw,
    workorder_type_clean,
    case
        when starts_with(workorder_type_clean, 'curative') then 'Curative'
        when starts_with(workorder_type_clean, 'preventive') then 'Preventive'
        when starts_with(workorder_type_clean, 'installation') then 'Installation'
        when starts_with(workorder_type_clean, 'desinstallation') then 'Desinstallation'
        else initcap(replace(workorder_type_clean, '_', ' '))
    end as workorder_type_grouped,
    machine_raw,
    machine_clean,
    famille_neshu,
    demand_description,
    workorder_report,
    workorder_motif_non_intervention,
    workorder_detail_non_intervention,
    workorder_raison_mise_en_pause,
    workorder_explication_mise_en_pause,
    workorder_technician_name,
    technician_equipe,

    -- Attributs client / site / matériel
    partner_name,
    client_code,
    client_name,
    client_category,
    client_is_active,
    site_code,
    site_name,
    site_address,
    postal_code_site as site_postal_code,
    material_serial_number,
    metropole as metropolitan,
    metropole_city,

    -- Tarification
    reccurence as recurrence_count,
    type_tarif as pricing_type,
    key_tarif_used as pricing_key_used,
    a_facturer as to_invoice,
    montant as amount,
    prod as prod_number,
    case
        when workorder_id is null and a_facturer = true then 'UNTRACKABLE'
        when montant is not null and a_facturer = true then 'VALIDATED'
        when montant is null and a_facturer = true then 'MISSING_TARIF'
        else 'NOT_BILLABLE'
    end as billing_validation_status,

    -- Délai
    date_creation_ref,
    delai_jours_ouvres,
    type_delai,

    -- Dates
    demand_created_at,
    demand_updated_at,
    workorder_date_creation,
    date_planned,
    date_started,
    date_done

from final_table
