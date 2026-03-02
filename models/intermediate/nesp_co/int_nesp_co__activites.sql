{{ config(
    materialized = 'table',
    description='Liste des Activités des commerciaux',
    tags = ['intermediate', 'nesp_co', 'activites']
) }}

with base as (

    select
        -- Identifiants bruts
        activity_id,
        employee_responsible,
        activity_type,
        activity_category,
        type_role,
        activity_life_cycle_status,

        -- Données descriptives
        notes,
        main_account,
        calendar_month,

        -- IDs nettoyés
        c4c_id_main_account     as act_compte_id,
        nessoft_id_main_account as act_id_nessoft,

        -- Créateur unifié
        case
            when activity_type = 'Phone Call' then created_by_phone_call
            when activity_type = 'Appointment' then created_by_appointment
            else employee_responsible
        end as act_cree_par,

        -- Nom activité unifié
        case
            when activity_type = 'Phone Call' then phone_call
            when activity_type = 'Appointment' then appointment
            else null
        end as act_nom,

        -- Date début unifiée
        case
            when activity_type = 'Phone Call' then start_date_phone_call
            when activity_type = 'Appointment' then start_date_appointment
            else start_date_task
        end as act_date_debut

    from {{ref('stg_nesp_co__activite')}}
),

enrich as (

    select

        -- Clés
        activity_id              as act_id,
        employee_responsible     as act_id_resp,
        act_cree_par,

        -- Compte
        main_account             as act_compte_nom,
        act_compte_id,
        act_id_nessoft,

        -- Contenu
        notes                    as act_note,
        calendar_month           as act_mois_creation,
        act_nom,
        act_date_debut,

        -- Traduction type
        case activity_type
            when 'Appointment'   then 'Rendez-vous'
            when 'Activity Task' then 'Tâche dactivité'
            when 'Phone Call'    then 'Appel téléphonique'
            when 'E-Mail'        then 'e-mail'
            else activity_type
        end as act_type,

        -- Traduction rôle
        case type_role
            when 'Customer' then 'Client'
            when 'Prospect' then 'Client potentiel'
            else type_role
        end as act_role,
        -- Traduction statut
        case activity_life_cycle_status
            when 'Open'       then 'En cours'
            when 'In Process' then 'En cours'
            when 'Completed'  then 'Terminé'
            else activity_life_cycle_status
        end as act_statut,

        -- Traduction catégorie
        case activity_category
            when 'Preparation' then 'Préparation'
            when 'Meeting' then 'Réunion'
            when 'Annual Visit' then 'Visite annuelle'
            when 'Telephone Call' then 'Appel téléphonique'
            when 'Prospecting' then 'Prospection'
            when '86/Not assigned' then '86/Non affecté'
            when 'Business E-Mail' then 'e-mail commercial'
            when 'Sales Call' then 'Visite'
            when 'Customer follow-up' then 'Suivi client'
            when 'Customer Request' then 'Demande client'
            when 'Downgrader/Fragile' then 'Client baissier/fragile'
            when 'R5: Client follow-up and retention'
                then 'R5: Suivis client fidélisation'
            when 'R1: Discovery' then 'R1: Decouverte'
            when 'R3: Negotiation' then 'R3: Negociation'
            when 'Customer Complaint' then 'Plainte client'
            when 'Opportunity follow-up' then 'Relance opportunité'
            when 'R2: Animation' then 'R2: Animation'
            when 'R4: Signing' then 'R4: Signature'
            when 'Visit' then 'Visite'
            when 'Workshop' then 'R2: Animation / Events'
            else activity_category
        end as act_categorie

    from base

)

select

    act_id,
    act_id_resp,
    act_cree_par,
    act_compte_nom,
    act_compte_id,
    act_id_nessoft,
    act_note,
    act_mois_creation,
    act_nom,
    act_date_debut,
    act_type,
    act_role,
    act_statut,
    act_categorie,

    case
        when act_date_debut is null then null
        else format(
            '%02d.%04d',
            extract(isoweek from act_date_debut),
            extract(isoyear from act_date_debut)
        )
    end as act_semaine_creation

from enrich

where not (
    act_id is null
    and act_compte_id is null
    and act_nom is null
    and act_date_debut is null
)