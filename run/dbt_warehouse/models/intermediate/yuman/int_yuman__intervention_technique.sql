
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_yuman__intervention_technique`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions Yuman r\u00e9alis\u00e9es, tous partenaires, au format commun des interventions techniques, partag\u00e9 avec int_nesp_tech__intervention_technique : une ligne par intervention, avec sa facturation, son d\u00e9lai et ses drapeaux de primes.\n[COMMENT CONSTRUITE] int_yuman__interventions filtr\u00e9 sur intervention_state = 'REALISEE', renomm\u00e9 au format commun. Pas de bonus ni de d\u00e9lai en heures ouvr\u00e9es c\u00f4t\u00e9 Yuman : bonus_bool = false, delai_heures_* = 0, delai_tech = delai_partenaire = type_delai.\n[GRAIN] 1 ligne par intervention. PK = key_inter ('YUMAN_' + workorder_id).\n[NOTES] Lu par fct_technique__intervention, qui l'unit \u00e0 la branche Nomad : m\u00eame liste de colonnes, dont le sens par source est d\u00e9crit dans ce mart. Le technicien est rattach\u00e9 dans le mart, avec celui de la branche Nomad.\n"""
    )
    as (
      

with interventions_yuman as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
),

-- Seules les interventions réalisées comptent en facturation et en primes.
interventions as (
    select
        'YUMAN' as src_inter,
        partner_name as partenaire,
        -- Le découpage par agence est propre au contrat Nespresso : pas d'agence côté Yuman.
        cast(null as string) as agency,
        cast(workorder_id as string) as intervention_id,
        workorder_number as numero_pu,
        cast(technician_id as string) as tech_id,
        client_code as client_id,
        initcap(client_name) as client_nom,
        machine_clean as categorie_machine,
        machine_raw as machine_clean,
        workorder_type_grouped as type_intervention,
        material_serial_number as num_serie_machine,
        workorder_status as intervention_statut,
        billing_validation_status as statut_facturation,
        timestamp(workorder_date_creation) as date_creation,
        date_started as date_debut,
        date_done as date_fin,
        pricing_key_used as key_factu,
        site_postal_code as code_postal,
        demand_description as consignes,
        workorder_report as commentaire_tech,
        prod_number as prod,
        amount as montant,
        -- Ni bonus ni délai en heures ouvrées côté Yuman.
        false as bonus_bool,
        amount as montant_avec_bonus,
        cast(0 as float64) as delai_heures_debut,
        cast(0 as float64) as delai_heures_fin,
        type_delai as delai_tech,
        type_delai as delai_partenaire,
        upper(split(pricing_key_used, '_')[0]) as alias_obj_type_inter,
        upper(split(pricing_key_used, '_')[1]) as alias_obj_type_machine,
        upper(split(pricing_key_used, '_')[1]) as alias_obj_grp_machine
    from interventions_yuman
    where intervention_state = 'REALISEE'
),

-- Mêmes colonnes, dans le même ordre, que int_nesp_tech__intervention_technique :
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
        -- La prime montagne est une règle de la grille Nespresso (clé « … Aguila … Montagne »).
        0 as flag_montagne_prime,
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
)

select * from final
    );
  