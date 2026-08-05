

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_details`
),

renamed as (
    select
        -- primary key + foreign key to stg_zoho_desk__tickets
        -- La FK est nommée par dlt d'après la ressource PARENTE
        -- (`zoho_desk_associated_tickets`). Elle s'appelait _zoho_desk_tickets_id
        -- jusqu'au passage du pipeline sur dlt.sources.rest_api : le transformer
        -- était alors défini sur une ressource nommée `tickets`.
        _zoho_desk_associated_tickets_id as ticket_id,

        -- sla (indicateurs et flags regroupés par domaine)
        sla_id,
        is_over_due,
        is_response_overdue,
        is_escalated,

        -- layout
        layout_id,
        layout_details__layout_name as layout_name,

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
        -- Colonnes source préfixées cf__ : l'API rend un OBJET `cf`, que dlt
        -- aplatit en cf__<nom d'API>. L'ancien pipeline remontait ces clés à la
        -- racine à la main — de la logique dans l'extracteur, que les conventions
        -- d'ingestion interdisent. Les noms de SORTIE ne bougent pas : les couches
        -- intermediate et marts sont intactes.
        cf__cf_statut_client as cf_statut_client,
        cf__cf_nature_des_demandes as cf_nature_des_demandes,
        cf__cf_type as cf_type,
        cf__cf_type_de_remboursement as cf_type_de_remboursement,
        cf__cf_votre_demande_concerne as cf_votre_demande_concerne,
        cf__cf_demande_intervention as cf_demande_intervention,
        cf__cf_technique as cf_technique,
        cf__cf_s_equipements as cf_s_equipements,
        cf__cf_machines as cf_machines,
        cf__cf_suivi_intervention as cf_suivi_intervention,
        cf__cf_s_facturation as cf_s_facturation,
        cf__cf_s_remboursement as cf_s_remboursement,
        cf__cf_s_commercial as cf_s_commercial,
        cf__cf_s_reappro as cf_s_reappro,
        cf__cf_rupture as cf_rupture,
        cf__cf_boissons_chaudes as cf_boissons_chaudes,
        cf__cf_snack as cf_snack,
        cf__cf_consommables as cf_consommables,
        cf__cf_s_gestion_des_cartes_privatives as cf_s_gestion_des_cartes_privatives,
        cf__cf_s_gestion_des_planogrammes as cf_s_gestion_des_planogrammes,
        cf__cf_s_recyclage as cf_s_recyclage,
        cf__cf_s_systeme_de_paiement as cf_s_systeme_de_paiement,
        cf__cf_collecte as cf_collecte,
        cf__cf_badges as cf_badges,
        cf__cf_inscription as cf_inscription,
        cf__cf_creation_d_un_site as cf_creation_d_un_site,
        cf__cf_modification_d_un_site as cf_modification_d_un_site,
        cf__cf_nom_de_l_entreprise as cf_nom_de_l_entreprise,
        cf__cf_secteur_d_activite as cf_secteur_d_activite,
        cf__cf_civilite as cf_civilite,
        cf__cf_nom as cf_nom,
        cf__cf_prenom as cf_prenom,
        cf__cf_numero_de_telephone as cf_numero_de_telephone,
        cf__cf_date_de_l_animation as cf_date_de_l_animation,
        cf__cf_champ_machine_formulaire as cf_champ_machine_formulaire,
        cf__cf_previous_status as cf_previous_status,
        cf__cf_close_ticket_notification_sent as cf_close_ticket_notification_sent,
        cf__cf_contrat_avenant as cf_contrat_avenant,
        cf__cf_correction as cf_correction,
        cf__cf_modification as cf_modification,
        cf__cf_s_remontees_personnel_neshu as cf_s_remontees_personnel_neshu,
        cf__cf_s_commande_directes as cf_s_commande_directes,
        cf__cf_tranche_effectiv as cf_tranche_effectiv,
        cf__cf_fiche_de_renseignement as cf_fiche_de_renseignement,
        cf__cf_boisson_chaude as cf_boisson_chaude,
        cf__cf_modifications as cf_modifications,

        -- metadata
        created_by,
        modified_by,
        onhold_time

    from source
)

select * from renamed