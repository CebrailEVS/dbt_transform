{{ config(
    materialized='table',
    schema='marts',
    alias='fct_technique__suivi_partenaire',
    partition_by={"field": "date_done", "data_type": "timestamp"},
    cluster_by=['partner_name', 'demand_status', 'workorder_status']
) }}

/*
    Modèle de suivi opérationnel par partenaire — remplace le DAG MAIL_Yuman_Module_Notification.

    Chaque ligne = une demande d'intervention enrichie avec son workorder associé.
    La colonne intervention_state (état métier canonique, défini une seule fois dans
    int_yuman__demands_workorders_enriched) est la source de vérité de l'état. Les 7
    premiers flags d'alerte en sont de simples projections booléennes — aucune logique
    d'état n'est recalculée ici, ce qui garantit l'iso avec int_yuman__interventions.

    Flags dérivés de l'état canonique :
      - alerte_demand_open           : intervention_state = DEMANDE_OUVERTE (demande sans workorder, Open)
      - alerte_demand_accepted       : demand_status = Accepted (statut de la demande, orthogonal à l'état)
      - alerte_demand_rejected       : intervention_state = DEMANDE_REJETEE (demande sans workorder, Rejected)
      - alerte_non_realisee          : intervention_state = NON_REALISEE (workorder Closed AVEC motif)
      - alerte_planifiee             : intervention_state = PLANIFIEE (Scheduled)
      - alerte_cloturee              : intervention_state = REALISEE (vraie clôture, sans motif de non-intervention)
      - alerte_pause                 : intervention_state = EN_PAUSE (pause active : In progress avec champ de pause)

    Alertes opérationnelles spécifiques (hors état canonique, logique temporelle / SLA) :
      - alerte_en_cours_non_cloture  : démarrée avant aujourd'hui, toujours ouverte (staleness)
      - alerte_hors_delais           : curative ouverte > 3 jours ouvrés (logique NESHU, extensible)

    Usage Power BI : filtrer sur partner_name, technician_equipe, demand_created_at, intervention_state, flags d'alerte.
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
    base.intervention_state,
    base.is_orphan_workorder,
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
    -- FLAGS D'ALERTE — dérivés de l'état canonique intervention_state
    -- (source de vérité unique : int_yuman__demands_workorders_enriched)
    -- -------------------------------------------------------------------------

    -- 1. Demande créée par le client, pas encore traitée (aucun workorder rattaché)
    base.intervention_state = 'DEMANDE_OUVERTE' as alerte_demand_open,

    -- 2. Demande acceptée par le support (statut de la demande, orthogonal à l'état d'intervention)
    base.demand_status = 'Accepted' as alerte_demand_accepted,

    -- 3. Demande rejetée par le support (aucun workorder rattaché)
    base.intervention_state = 'DEMANDE_REJETEE' as alerte_demand_rejected,

    -- 4. Intervention non réalisée (workorder clôturé AVEC motif/détail de non-intervention)
    base.intervention_state = 'NON_REALISEE' as alerte_non_realisee,

    -- 5. Intervention planifiée (Scheduled)
    base.intervention_state = 'PLANIFIEE' as alerte_planifiee,

    -- 6. Intervention clôturée et réalisée (vraie clôture, sans motif de non-intervention)
    base.intervention_state = 'REALISEE' as alerte_cloturee,

    -- 7. Intervention actuellement en pause (In progress avec champ de pause renseigné)
    base.intervention_state = 'EN_PAUSE' as alerte_pause,

    -- -------------------------------------------------------------------------
    -- ALERTES OPÉRATIONNELLES SPÉCIFIQUES — hors état canonique
    -- (logique temporelle / SLA, sans équivalent dans intervention_state)
    -- -------------------------------------------------------------------------

    -- 8. Intervention démarrée avant aujourd'hui et toujours ouverte (staleness)
    (
        base.workorder_status != 'Closed'
        and base.date_started is not null
        and date(base.date_started) < current_date('Europe/Paris')
    ) as alerte_en_cours_non_cloture,

    -- 9. Curative hors délais : > 3 jours ouvrés depuis la création, encore ouverte
    coalesce(bde.nb_jours_ouvrables_ecoules, 0) > 3 as alerte_hors_delais

from base
left join business_days_elapsed as bde
    on base.demand_id = bde.demand_id
