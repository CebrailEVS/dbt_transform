{{ config(
    materialized='table',
    schema='marts',
    alias='fct_yuman__suivi_partenaires',
    partition_by={"field": "demand_created_at", "data_type": "timestamp"},
    cluster_by=['partner_name', 'demand_status', 'workorder_status']
) }}

/*
    Modèle de suivi opérationnel par partenaire — remplace le DAG MAIL_Yuman_Module_Notification.

    Chaque ligne = une demande d'intervention enrichie avec son workorder associé.
    Les 7 flags d'alerte correspondent aux 7 types d'emails de l'ancien DAG :
      - alerte_rejet                 : demande rejetée ou non-intervention
      - alerte_acceptee              : demande acceptée, pas encore planifiée
      - alerte_planifiee             : intervention planifiée (Scheduled)
      - alerte_cloturee              : intervention clôturée (vraie clôture)
      - alerte_pause                 : intervention mise en pause
      - alerte_en_cours_non_cloture  : démarrée avant aujourd'hui, toujours ouverte
      - alerte_hors_delais           : curative ouverte > 3 jours ouvrés (logique NESHU, extensible)

    Usage Power BI : filtrer sur partner_name, technician_equipe, demand_created_at, flags d'alerte.
*/

with base as (
    select *
    from {{ ref('int_yuman__demands_workorders_enriched') }}
),

jours_feries as (
    select date_ferie
    from {{ ref('ref_general__feries_metropole') }}
),

-- Calcul du nombre de jours ouvrés écoulés depuis la création de la demande,
-- uniquement pour les curatives encore ouvertes (logique hors délais NESHU).
business_days_elapsed as (
    select
        b.demand_id,
        count(calendar_day) - 1 as nb_jours_ouvrables_ecoules
    from base as b,
        unnest(
            generate_date_array(
                date(b.demand_created_at),
                current_date('Europe/Paris'),
                interval 1 day
            )
        ) as calendar_day
    left join jours_feries as jf
        on calendar_day = jf.date_ferie
    where
        b.workorder_type = 'Reactive'
        and b.demand_status = 'Accepted'
        and b.workorder_status != 'Closed'
        and b.workorder_motif_non_intervention is null
        and b.workorder_detail_non_intervention is null
        and b.workorder_raison_mise_en_pause is null
        and b.workorder_explication_mise_en_pause is null
        and extract(dayofweek from calendar_day) not in (1, 7)
        and jf.date_ferie is null
    group by b.demand_id
)

select
    -- Identifiants
    base.demand_id,
    base.workorder_id,
    base.material_id,
    base.site_id,
    base.client_id,
    base.technician_id,

    -- Partenaire & Client
    base.partner_name,
    base.client_code,
    base.client_name,
    base.client_category,
    base.client_is_active,

    -- Site
    base.site_code,
    base.site_name,
    base.site_address,
    base.site_postal_code,

    -- Contact du site
    base.contact_name,
    base.contact_phone,
    base.contact_mobile,
    base.contact_email,

    -- Matériel
    base.material_name,
    base.material_serial_number,
    base.material_brand,
    base.material_description,
    base.material_category,

    -- Demande d'intervention
    base.demand_description,
    base.demand_status,
    base.demand_reject_comment,
    base.demand_created_at,
    base.demand_updated_at,
    base.demand_category_name,

    -- Intervention (workorder)
    base.workorder_number,
    base.workorder_type,
    base.workorder_status,
    base.workorder_technician_name,
    base.technician_equipe,
    base.workorder_report,
    base.workorder_date_creation,
    base.workorder_motif_non_intervention,
    base.workorder_detail_non_intervention,
    base.workorder_raison_mise_en_pause,
    base.workorder_explication_mise_en_pause,
    base.workorder_necessite_intervenir,
    base.workorder_si_non_pourquoi,
    base.date_planned,
    base.date_started,
    base.date_done,

    -- Indicateurs de durée (utiles pour les visuels Power BI)
    date_diff(
        current_date('Europe/Paris'),
        date(base.demand_created_at),
        day
    ) as nb_jours_depuis_creation,

    coalesce(bde.nb_jours_ouvrables_ecoules, 0) as nb_jours_ouvrables_ecoules,

    -- -------------------------------------------------------------------------
    -- FLAGS D'ALERTE — correspondent aux 7 types d'emails de l'ancien DAG
    -- -------------------------------------------------------------------------

    -- 1. Demande rejetée ou non-intervention
    case
        when
            base.demand_status = 'Rejected'
            or base.workorder_motif_non_intervention is not null
            or base.workorder_detail_non_intervention is not null
        then true
        else false
    end as alerte_rejet,

    -- 2. Demande acceptée mais pas encore planifiée (statut Open)
    case
        when base.demand_status = 'Open'
        then true
        else false
    end as alerte_acceptee,

    -- 3. Intervention planifiée (technicien et date assignés)
    case
        when base.workorder_status = 'Scheduled'
        then true
        else false
    end as alerte_planifiee,

    -- 4. Intervention clôturée (vraie clôture, sans motif de non-intervention)
    case
        when
            base.workorder_status = 'Closed'
            and base.workorder_motif_non_intervention is null
            and base.workorder_detail_non_intervention is null
        then true
        else false
    end as alerte_cloturee,

    -- 5. Intervention mise en pause
    case
        when
            base.workorder_raison_mise_en_pause is not null
            or base.workorder_explication_mise_en_pause is not null
        then true
        else false
    end as alerte_pause,

    -- 6. Intervention en cours non clôturée (démarrée avant aujourd'hui, toujours ouverte)
    case
        when
            base.workorder_status not in ('Closed', 'Cancelled')
            and base.date_started is not null
            and date(base.date_started) < current_date('Europe/Paris')
        then true
        else false
    end as alerte_en_cours_non_cloture,

    -- 7. Curative hors délais : > 3 jours ouvrés depuis la création, encore ouverte
    case
        when coalesce(bde.nb_jours_ouvrables_ecoules, 0) > 3
        then true
        else false
    end as alerte_hors_delais

from base
left join business_days_elapsed as bde
    on base.demand_id = bde.demand_id
