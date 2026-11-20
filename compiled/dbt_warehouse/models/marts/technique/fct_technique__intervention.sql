

-- La logique propre à chaque source vit dans les deux intermédiaires ; ce mart ne garde que
-- ce qui croise les sources (union, technicien Nomad retrouvé dans Yuman). Colonnes nommées
-- pour que l'union ne dépende pas de l'ordre des colonnes des intermédiaires.

with interventions as (
    select key_inter, src_inter, partenaire, agency, intervention_id, numero_pu, intervention_statut, statut_facturation, categorie_machine, machine_clean, type_intervention, num_serie_machine, tech_id, client_id, client_nom, date_creation, date_debut, date_fin, duree_inter_minutes, key_factu, code_postal, consignes, commentaire_tech, prod, montant, bonus_bool, montant_avec_bonus, delai_heures_debut, delai_heures_fin, delai_tech, delai_partenaire, flag_montagne_prime, flag_paris_intramuros, flag_hors_delai_tech, alias_obj_type_inter, alias_obj_type_machine, alias_obj_grp_machine
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__intervention_technique`
    union all
    select key_inter, src_inter, partenaire, agency, intervention_id, numero_pu, intervention_statut, statut_facturation, categorie_machine, machine_clean, type_intervention, num_serie_machine, tech_id, client_id, client_nom, date_creation, date_debut, date_fin, duree_inter_minutes, key_factu, code_postal, consignes, commentaire_tech, prod, montant, bonus_bool, montant_avec_bonus, delai_heures_debut, delai_heures_fin, delai_tech, delai_partenaire, flag_montagne_prime, flag_paris_intramuros, flag_hors_delai_tech, alias_obj_type_inter, alias_obj_type_machine, alias_obj_grp_machine
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__intervention_technique`
),

techniciens as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
),

final as (
    select
        i.key_inter,
        i.src_inter,
        i.partenaire,
        i.agency,
        i.intervention_id,
        i.numero_pu,
        i.intervention_statut,
        i.statut_facturation,
        i.categorie_machine,
        i.machine_clean,
        i.type_intervention,
        i.num_serie_machine,
        i.tech_id,
        i.client_id,
        i.client_nom,
        i.date_creation,
        i.date_debut,
        i.date_fin,
        i.duree_inter_minutes,
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
        i.flag_montagne_prime,
        i.flag_paris_intramuros,
        i.flag_hors_delai_tech,
        tech.user_id as tech_yuman_id,
        tech.nomad_id as tech_nomad_id,
        tech.user_name as tech_nom,
        tech.user_secteur as tech_secteur,
        i.alias_obj_type_inter,
        i.alias_obj_type_machine,
        i.alias_obj_grp_machine
    from interventions as i
    -- Le technicien d'une intervention Nomad est retrouvé dans Yuman par son id Nomad.
    left join techniciens as tech
        on (
            (i.src_inter = 'NESP' and lower(tech.nomad_id) = i.tech_id)
            or (i.src_inter = 'YUMAN' and cast(tech.user_id as string) = i.tech_id)
        )
)

select * from final