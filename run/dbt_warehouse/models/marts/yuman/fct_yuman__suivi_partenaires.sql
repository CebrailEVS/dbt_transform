
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_yuman__suivi_partenaires`
      
    partition by timestamp_trunc(date_done, day)
    cluster by partner_name, demand_status, workorder_status

    
    OPTIONS(
      description="""Table de suivi op\u00e9rationnel des demandes et interventions par partenaire. Remplace le DAG Airflow MAIL_Yuman_Module_Notification (emails automatiques). Chaque ligne correspond \u00e0 une demande d'intervention enrichie avec son workorder, son client, son site, son mat\u00e9riel et son technicien. Les 7 flags d'alerte (alerte_*) permettent de filtrer dans Power BI chaque type de situation qui d\u00e9clenchait auparavant un email.\n"""
    )
    as (
      

/*
    Modèle de suivi opérationnel par partenaire — remplace le DAG MAIL_Yuman_Module_Notification.

    Chaque ligne = une demande d'intervention enrichie avec son workorder associé.
    Les 9 flags d'alerte sont séparés entre statut de la demande et statut du workorder :

    Statut de la demande (demand_status) :
      - alerte_demand_open           : demande créée par le client, pas encore traitée par le support
      - alerte_demand_accepted       : demande acceptée par le support
      - alerte_demand_rejected       : demande rejetée par le support

    Statut du workorder :
      - alerte_workorder_canceled    : demande acceptée mais workorder annulé (motif/détail non-intervention renseigné)
      - alerte_planifiee             : intervention planifiée (Scheduled)
      - alerte_cloturee              : intervention clôturée (vraie clôture, sans motif de non-intervention)
      - alerte_pause                 : intervention mise en pause
      - alerte_en_cours_non_cloture  : démarrée avant aujourd'hui, toujours ouverte
      - alerte_hors_delais           : curative ouverte > 3 jours ouvrés (logique NESHU, extensible)

    Usage Power BI : filtrer sur partner_name, technician_equipe, demand_created_at, flags d'alerte.
*/

with base as (
    select *
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__demands_workorders_enriched`
),

jours_feries as (
    select date_ferie
    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`
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
    -- FLAGS D'ALERTE — statut de la demande
    -- -------------------------------------------------------------------------

    -- 1. Demande créée par le client, pas encore traitée par le support
    base.demand_status = 'Open' as alerte_demand_open,

    -- 2. Demande acceptée par le support
    base.demand_status = 'Accepted' as alerte_demand_accepted,

    -- 3. Demande rejetée par le support
    base.demand_status = 'Rejected' as alerte_demand_rejected,

    -- -------------------------------------------------------------------------
    -- FLAGS D'ALERTE — statut du workorder
    -- -------------------------------------------------------------------------

    -- 4. Demande acceptée mais workorder annulé (motif ou détail de non-intervention renseigné)
    (
        base.demand_status = 'Accepted'
        and (
            base.workorder_motif_non_intervention is not null
            or base.workorder_detail_non_intervention is not null
        )
    ) as alerte_workorder_canceled,

    -- 5. Intervention planifiée (technicien et date assignés)
    base.workorder_status = 'Scheduled' as alerte_planifiee,

    -- 6. Intervention clôturée (vraie clôture, sans motif de non-intervention)
    (
        base.workorder_status = 'Closed'
        and base.workorder_motif_non_intervention is null
        and base.workorder_detail_non_intervention is null
    ) as alerte_cloturee,

    -- 7. Intervention mise en pause
    (
        base.workorder_raison_mise_en_pause is not null
        or base.workorder_explication_mise_en_pause is not null
    ) as alerte_pause,

    -- 8. Intervention en cours non clôturée (démarrée avant aujourd'hui, toujours ouverte)
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
    );
  