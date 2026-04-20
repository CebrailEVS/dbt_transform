
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_details`
      
    
    

    
    OPTIONS(
      description="""Enrichissement 1:1 par ticket : flags SLA, champs custom (cf_*), r\u00e9solution, layout. Source : prod_raw.zoho_desk_ticket_details Transformation : _zoho_desk_tickets_id renomm\u00e9 en ticket_id. NOTE sur le nom de la colonne source : la FK se nomme _zoho_desk_tickets_id (et non _zoho_desk_associated_tickets_id) car le transformer dlt \u00e9tait d\u00e9fini avec data_from=tickets. Dans le staging, on renomme en ticket_id pour la coh\u00e9rence.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_details`
),

renamed as (
    select
        -- primary key + foreign key to stg_zoho_desk__tickets
        -- (colonne source nommée _zoho_desk_tickets_id car le transformer dlt
        --  utilisait la ressource `tickets` comme parent)
        _zoho_desk_tickets_id as ticket_id,

        -- sla (indicateurs et flags regroupés par domaine)
        sla_id,
        is_over_due,
        is_response_overdue,
        is_escalated,

        -- layout
        layout_id,
        layout_name,

        -- resolution
        resolution,
        contract_id,

        -- engagement counts (STRING in source → INT64)
        safe_cast(follower_count as int64) as follower_count,
        safe_cast(tag_count as int64) as tag_count,
        safe_cast(approval_count as int64) as approval_count,
        safe_cast(time_entry_count as int64) as time_entry_count,
        safe_cast(task_count as int64) as task_count,

        -- custom fields (tous STRING — caster dans les marts si nécessaire)
        cf_statut_client,
        cf_nature_des_demandes,
        cf_type,
        cf_type_de_remboursement,
        cf_votre_demande_concerne,
        cf_demande_intervention,
        cf_technique,
        cf_s_equipements,
        cf_machines,
        cf_suivi_intervention,
        cf_s_facturation,
        cf_s_remboursement,
        cf_s_commercial,
        cf_s_reappro,
        cf_rupture,
        cf_boissons_chaudes,
        cf_snack,
        cf_consommables,
        cf_s_gestion_des_cartes_privatives,
        cf_s_gestion_des_planogrammes,
        cf_s_recyclage,
        cf_s_systeme_de_paiement,
        cf_collecte,
        cf_badges,
        cf_inscription,
        cf_creation_d_un_site,
        cf_modification_d_un_site,
        cf_nom_de_l_entreprise,
        cf_secteur_d_activite,
        cf_civilite,
        cf_nom,
        cf_prenom,
        cf_numero_de_telephone,
        cf_date_de_l_animation,
        cf_champ_machine_formulaire,
        cf_previous_status,
        cf_close_ticket_notification_sent,
        cf_contrat_avenant,
        cf_correction,
        cf_modification,
        cf_s_remontees_personnel_neshu,
        cf_s_commande_directes,
        cf_tranche_effectiv,
        cf_fiche_de_renseignement,
        cf_boisson_chaude,
        cf_modifications,

        -- metadata
        created_by,
        modified_by,
        onhold_time

    from source
)

select * from renamed
    );
  