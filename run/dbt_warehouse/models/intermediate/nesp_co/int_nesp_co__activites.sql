
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__activites`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Activit\u00e9s commerciales Nespresso men\u00e9es par les commerciaux (appels, rendez-vous, t\u00e2ches, e-mails) : une ligne par activit\u00e9, avec son type, sa cat\u00e9gorie, son statut et le compte vis\u00e9.\n[COMMENT CONSTRUITE] stg_nesp_co__activite : unification des champs selon le type d'activit\u00e9 (Phone Call / Appointment / Task \u2014 nom, cr\u00e9ateur, date de d\u00e9but), puis traduction FR du type, du r\u00f4le, du statut et de la cat\u00e9gorie.\n[GRAIN] 1 ligne par act_id (PK). ~140,5k lignes.\n[NOTES] Source commerciale Nespresso (WIP). Certaines cat\u00e9gories C4C restent non traduites (ex. 'Customer Visit') quand elles ne sont pas couvertes par le mapping.\n"""
    )
    as (
      

with base as (

    select
        -- Identifiants bruts
        activity_id,
        c4c_id_commercial,
        activity_type,
        activity_category,
        type_role,
        activity_life_cycle_status,

        -- Données descriptives
        notes,
        main_account,
        calendar_month,

        -- IDs nettoyés
        c4c_id_main_account as act_compte_id,
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
        end as act_nom,

        -- Date début unifiée
        case
            when activity_type = 'Phone Call' then start_date_phone_call
            when activity_type = 'Appointment' then start_date_appointment
            else start_date_task
        end as act_date_debut

    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__activite`
),

enrich as (

    select

        -- Clés
        activity_id as act_id,
        c4c_id_commercial as act_id_resp,
        act_cree_par,

        -- Compte
        main_account as act_compte_nom,
        act_compte_id,
        act_id_nessoft,

        -- Contenu
        notes as act_note,
        calendar_month as act_mois_creation,
        act_nom,
        act_date_debut,

        -- Traduction type
        case activity_type
            when 'Appointment' then 'Rendez-vous'
            when 'Activity Task' then 'Tâche dactivité'
            when 'Phone Call' then 'Appel téléphonique'
            when 'E-Mail' then 'e-mail'
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
            when 'Open' then 'En cours'
            when 'In Process' then 'En cours'
            when 'Completed' then 'Terminé'
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
    );
  