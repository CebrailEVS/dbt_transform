
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__intervention_technique`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions Nespresso (Nomad Repair) au format commun des interventions techniques, partag\u00e9 avec int_yuman__intervention_technique : une ligne par intervention non annul\u00e9e, avec sa facturation, ses d\u00e9lais et ses drapeaux de primes.\n[COMMENT CONSTRUITE] int_nesp_tech__interventions_dedup + LEFT JOIN int_nesp_tech__facturation_interventions et int_nesp_tech__delais_interventions sur n_planning ; zone montagne lue dans le seed ref_nesp_tech__cps_montagne_primes. Filtre : etat_intervention != 'annul\u00e9e'.\n[GRAIN] 1 ligne par intervention. PK = key_inter ('NESP_' + n_planning).\n[NOTES] Lu par fct_technique__intervention, qui l'unit \u00e0 la branche Yuman : m\u00eame liste de colonnes, dont le sens par source est d\u00e9crit dans ce mart. Le technicien n'est pas r\u00e9solu ici : son id Nomad se retrouve dans les utilisateurs Yuman, ce qui croise les sources.\n"""
    )
    as (
      

with dedup as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
),

facturation as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__facturation_interventions`
),

delais as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__delais_interventions`
),

cp_montagne as (
    select * from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__cps_montagne_primes`
),

-- Annulées exclues : elles ne comptent ni en facturation ni en primes.
interventions as (
    select
        'NESP' as src_inter,
        'NESPRESSO' as partenaire,
        -- Agence lue au dédup et non à la facturation : présente même quand la clé de
        -- facturation n'a pas été résolue.
        dedup.agency,
        cast(dedup.n_planning as string) as intervention_id,
        cast(dedup.n_planning as string) as numero_pu,
        dedup.n_tech as tech_id,
        cast(dedup.n_client as string) as client_id,
        initcap(dedup.raison_sociale_client) as client_nom,
        facturation.categorie_machine,
        facturation.machine_clean,
        facturation.type_inter_libelle as type_intervention,
        dedup.num_serie_machine,
        dedup.etat_intervention as intervention_statut,
        case
            when
                dedup.etat_intervention in ('terminée signée', 'signature différée', 'terminée non signée')
                then 'VALIDATED'
            when dedup.etat_intervention in ('mise en échec') then 'NOT VALIDATED'
            else 'NOT DEFINED'
        end as statut_facturation,
        dedup.pickup_date as date_creation,
        dedup.date_heure_debut as date_debut,
        dedup.date_heure_fin as date_fin,
        facturation.key_factu,
        dedup.code_postal_site as code_postal,
        dedup.consignes,
        dedup.observations as commentaire_tech,
        facturation.prod_factu as prod,
        facturation.tarif_factu as montant,
        delais.delai_bonus_bool as bonus_bool,
        facturation.tarif_factu + delais.delai_bonus_valeur as montant_avec_bonus,
        delais.delai_heures_debut,
        delais.delai_heures_fin,
        delais.type_delai_debut as delai_tech,
        delais.type_delai_fin as delai_partenaire,
        facturation.alias_obj_type_inter,
        facturation.alias_obj_type_machine,
        facturation.alias_obj_grp_machine
    from dedup
    left join facturation
        on dedup.n_planning = facturation.n_planning
    left join delais
        on dedup.n_planning = delais.n_planning
    where dedup.etat_intervention != 'annulée'
),

-- Mêmes colonnes, dans le même ordre, que int_yuman__intervention_technique :
-- fct_technique__intervention les met bout à bout.
final as (
    select
        concat(i.src_inter, '_', i.intervention_id) as key_inter,
        i.src_inter,
        i.partenaire,
        i.agency,
        i.intervention_id,
        i.numero_pu,
        i.intervention_statut,
        i.statut_facturation,
        coalesce(i.categorie_machine, 'UNDEFINED') as categorie_machine,
        coalesce(i.machine_clean, 'UNDEFINED') as machine_clean,
        i.type_intervention,
        i.num_serie_machine,
        i.tech_id,
        i.client_id,
        i.client_nom,
        i.date_creation,
        i.date_debut,
        i.date_fin,
        timestamp_diff(i.date_fin, i.date_debut, minute) as duree_inter_minutes,
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
        case
            when
                i.key_factu like '%Aguila%'
                and i.key_factu like '%Montagne%'
                and cp_montagne.montagne = 1
                then 1
            else 0
        end as flag_montagne_prime,
        case
            when starts_with(i.code_postal, '75') then 1
            else 0
        end as flag_paris_intramuros,
        case
            when
                i.delai_tech in ('J++', 'J+3')
                and lower(i.key_factu) like '%curative%'
                then 1
            else 0
        end as flag_hors_delai_tech,
        i.alias_obj_type_inter,
        i.alias_obj_type_machine,
        i.alias_obj_grp_machine
    from interventions as i
    left join cp_montagne
        on safe_cast(i.code_postal as int64) = cp_montagne.cp
)

select * from final
    );
  