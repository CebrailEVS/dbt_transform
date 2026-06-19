
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention`
      
    partition by timestamp_trunc(date_debut, day)
    cluster by partenaire, src_inter

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nFait consolid\u00e9 des interventions techniques EVS, tous partenaires\nconfondus. 1 ligne = 1 intervention r\u00e9alis\u00e9e. Couvre Nespresso (via Nomad\nRepair, source `nesp_tech`) et les partenaires Yuman (NESHU, BRITA, AUUM,\nTWYD, NU, EXPRESSO, FONTAINCO, DAAN). Base des indicateurs op\u00e9rationnels,\nde la facturation partenaire et du calcul des primes techniciens (bonus\nd\u00e9lai, prime montagne, Paris intramuros).\n\n[COMMENT CONSTRUITE]\nUNION ALL de 2 cha\u00eenes homog\u00e9n\u00e9is\u00e9es au m\u00eame format, s\u00e9par\u00e9es par la\ncolonne `src_inter` (NESP / YUMAN) :\n- branche NESP : `int_nesp_tech__interventions_dedup` (1 ligne par\n  n_planning) + LEFT JOIN `int_nesp_tech__facturation_interventions`\n  (key_factu, montant, prod) + LEFT JOIN `int_nesp_tech__delais_interventions`\n  (SLA jours/heures, bonus) + LEFT JOIN seed `ref_nesp_tech__key_facturation`\n  (libell\u00e9s objets). Filtre de population : `etat_intervention != 'annul\u00e9e'`.\n- branche YUMAN : `int_yuman__interventions` \u2014 mod\u00e8le intermediate unique\n  portant tarification + d\u00e9lai + \u00e9tat m\u00e9tier (depuis la refacto PR #135 ;\n  remplace l'ancienne cha\u00eene fait\u2192fait `fct_technique__workorder_pricing` +\n  `fct_neshu__workorder_delai`). Filtre de population :\n  `intervention_state = 'REALISEE'` (workorder Closed et r\u00e9ellement effectu\u00e9).\nEnrichissement commun (CTE `interventions_enrichies`) : dur\u00e9e, flags primes\n(montagne / Paris / hors-d\u00e9lai), mapping technicien via `stg_yuman__users`\n(jointure conditionnelle : `nomad_id` c\u00f4t\u00e9 NESP, `user_id` c\u00f4t\u00e9 YUMAN) et\nseed `ref_nesp_tech__cps_montagne_primes`.\n\n[GRAIN]\n1 ligne par intervention. PK = `key_inter` = `intervention_id` + '_' + `partenaire`.\n\n[NOTES]\n- Filtres r\u00e9partis sur 3 niveaux (intermediate \u2192 CTE du fait \u2192 jointures\n  LEFT). C\u00f4t\u00e9 NESP, `montant` / `key_factu` / `prod` sont NULL hors \u00e9tats\n  termin\u00e9e sign\u00e9e / signature diff\u00e9r\u00e9e / mise en \u00e9chec ; `delai_*` et\n  `bonus_*` ne sont calcul\u00e9s que pour les agences IDF/Paris (filtre interne\n  \u00e0 `int_nesp_tech__delais_interventions`).\n- Plusieurs colonnes sont harmonis\u00e9es mais portent une s\u00e9mantique\n  diff\u00e9rente selon la source \u2014 voir les descriptions de `statut_facturation`,\n  `categorie_machine` / `machine_clean`, `delai_heures_*`, `bonus_bool`,\n  `delai_tech` / `delai_partenaire`, `alias_obj_*`.\n- Rafra\u00eechissement : aucun scheduler d\u00e9di\u00e9 \u00e0 la BU technique. Le mart se\n  reconstruit automatiquement via `source:<source>+` quand une source\n  upstream charge (refacto Option C) \u2014 pipeline EL `nesp_tech` (lundi 07:30,\n  cron `30 7 * * 1`) et `yuman` (01:00 en semaine, cron `0 1 * * 1-5`).\n"""
    )
    as (
      

-- CTE 1 : Interventions Nespresso enrichies (facturation + délais)
with nesp_interventions as (
    select
        'NESP' as src_inter,
        'NESPRESSO' as partenaire,
        cast(dedup.n_planning as string) as intervention_id,
        dedup.n_tech as tech_id,
        factu.categorie_machine,
        factu.machine_clean,
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
        factu.key_factu,
        dedup.code_postal_site as code_postal,
        dedup.consignes,
        dedup.observations as commentaire_tech,
        factu.prod_factu as prod,
        factu.tarif_factu as montant,
        delais.delai_bonus_bool as bonus_bool,
        factu.tarif_factu + delais.delai_bonus_valeur as montant_avec_bonus,
        delais.delai_heures_debut,
        delais.delai_heures_fin,
        delais.type_delai_debut as delai_tech,
        delais.type_delai_fin as delai_partenaire,
        key_factu_obj.alias_obj_type_inter,
        key_factu_obj.alias_obj_type_machine,
        key_factu_obj.alias_obj_grp_machine
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as dedup
    left join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__facturation_interventions` as factu
        on dedup.n_planning = factu.n_planning
    left join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__delais_interventions` as delais
        on dedup.n_planning = delais.n_planning
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation` as key_factu_obj
        on factu.key_factu = key_factu_obj.key_ref_inter
    where dedup.etat_intervention != 'annulée'
),

-- CTE 2 : Interventions Yuman normalisées au même format
yuman_interventions as (
    select
        'YUMAN' as src_inter,
        inter_yuman.partner_name as partenaire,
        inter_yuman.workorder_number as intervention_id,
        cast(inter_yuman.technician_id as string) as tech_id,
        inter_yuman.machine_clean as categorie_machine,
        inter_yuman.machine_raw as machine_clean,
        inter_yuman.workorder_status as intervention_statut,
        inter_yuman.billing_validation_status as statut_facturation,
        timestamp(inter_yuman.workorder_date_creation) as date_creation,
        inter_yuman.date_started as date_debut,
        inter_yuman.date_done as date_fin,
        inter_yuman.pricing_key_used as key_factu,
        inter_yuman.site_postal_code as code_postal,
        inter_yuman.demand_description as consignes,
        inter_yuman.workorder_report as commentaire_tech,
        inter_yuman.prod_number as prod,
        inter_yuman.amount as montant,
        false as bonus_bool,
        inter_yuman.amount as montant_avec_bonus,
        0 as delai_heures_debut,
        0 as delai_heures_fin,
        type_delai as delai_tech,
        type_delai as delai_partenaire,
        upper(split(inter_yuman.pricing_key_used, '_')[0]) as alias_obj_type_inter,
        upper(split(inter_yuman.pricing_key_used, '_')[1]) as alias_obj_type_machine,
        upper(split(inter_yuman.pricing_key_used, '_')[1]) as alias_obj_grp_machine
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions` as inter_yuman
    where inter_yuman.intervention_state = 'REALISEE'
),

-- CTE 3 : Union des deux sources homogénéisées
interventions as (
    select * from nesp_interventions
    union all
    select * from yuman_interventions
),

-- CTE 4 : Enrichissement métier (durée + flags + mapping techniciens)
interventions_enrichies as (
    select
        concat(i.intervention_id, '_', i.partenaire) as key_inter,
        i.src_inter,
        i.partenaire,
        i.intervention_id,
        i.intervention_statut,
        i.statut_facturation,
        i.categorie_machine,
        i.machine_clean,
        i.tech_id,
        i.date_creation,
        i.date_debut,
        i.date_fin,
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
        i.alias_obj_type_inter,
        i.alias_obj_type_machine,
        i.alias_obj_grp_machine,

        -- Calcul durée en minutes
        timestamp_diff(i.date_fin, i.date_debut, minute) as duree_inter_minutes,

        -- Flag montagne (prime spécifique)
        case
            when
                i.key_factu like '%Aguila%'
                and i.key_factu like '%Montagne%'
                and cp_montagne.montagne = 1
                then 1
            else 0
        end as flag_montagne_prime,

        -- Flag Paris intramuros
        case
            when starts_with(i.code_postal, '75') then 1
            else 0
        end as flag_paris_intramuros,

        -- Flag hors délai technicien
        case
            when
                i.delai_tech in ('J++', 'J+3')
                and lower(i.key_factu) like '%curative%'
                --and i.partenaire = 'NESPRESSO'
                then 1
            else 0
        end as flag_hors_delai_tech,

        -- Mapping technicien
        tech.user_id as tech_yuman_id,
        tech.nomad_id as tech_nomad_id,
        tech.user_name as tech_nom,
        tech.user_secteur as tech_secteur

    from interventions as i
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__users` as tech
        on (
            (i.src_inter = 'NESP' and lower(tech.nomad_id) = i.tech_id)
            or (i.src_inter = 'YUMAN' and cast(tech.user_id as string) = i.tech_id)
        )
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__cps_montagne_primes` as cp_montagne
        on safe_cast(i.code_postal as int64) = cp_montagne.cp
)

select
    key_inter,
    src_inter,
    partenaire,
    intervention_id,
    intervention_statut,
    statut_facturation,
    coalesce(categorie_machine, 'UNDEFINED') as categorie_machine,
    coalesce(machine_clean, 'UNDEFINED') as machine_clean,
    tech_id,
    date_creation,
    date_debut,
    date_fin,
    duree_inter_minutes,
    key_factu,
    code_postal,
    consignes,
    commentaire_tech,
    prod,
    montant,
    bonus_bool,
    montant_avec_bonus,
    delai_heures_debut,
    delai_heures_fin,
    delai_tech,
    delai_partenaire,
    flag_montagne_prime,
    flag_paris_intramuros,
    flag_hors_delai_tech,
    tech_yuman_id,
    tech_nomad_id,
    tech_nom,
    tech_secteur,
    alias_obj_type_inter,
    alias_obj_type_machine,
    alias_obj_grp_machine
from interventions_enrichies
    );
  