
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention`
      
    partition by timestamp_trunc(date_debut, day)
    cluster by partenaire, src_inter

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nFait consolid\u00e9 des interventions techniques EVS, tous partenaires\nconfondus. 1 ligne = 1 intervention : non annul\u00e9e c\u00f4t\u00e9 Nespresso (via Nomad\nRepair, source `nesp_tech`, mises en \u00e9chec comprises), r\u00e9alis\u00e9e c\u00f4t\u00e9 Yuman\n(NESHU, BRITA, AUUM, TWYD, NU, EXPRESSO, FONTAINCO, NAYKA, DAAN). Base des indicateurs op\u00e9rationnels,\nde la facturation partenaire et du calcul des primes techniciens (bonus\nd\u00e9lai, prime montagne, Paris intramuros).\n\n[COMMENT CONSTRUITE]\nUNION ALL de 2 interm\u00e9diaires au m\u00eame format, s\u00e9par\u00e9s par la colonne\n`src_inter` (NESP / YUMAN), qui portent chacun la logique de leur source\n(population, facturation, d\u00e9lais, dur\u00e9e, drapeaux de primes) :\n- `int_nesp_tech__intervention_technique` : d\u00e9dup + facturation + d\u00e9lais\n  Nomad, interventions non annul\u00e9es ;\n- `int_yuman__intervention_technique` : `int_yuman__interventions`,\n  interventions r\u00e9alis\u00e9es (workorder Closed et r\u00e9ellement effectu\u00e9).\nLe mart ne garde que ce qui croise les sources : l'union, et le\nrattachement du technicien via `stg_yuman__users` (`nomad_id` c\u00f4t\u00e9 NESP,\n`user_id` c\u00f4t\u00e9 YUMAN).\n\n[GRAIN]\n1 ligne par intervention. PK = `key_inter` = `src_inter` + '_' + `intervention_id`.\n\n[NOTES]\n- Filtres r\u00e9partis sur 3 niveaux (interm\u00e9diaires amont \u2192 interm\u00e9diaires\n  au format commun \u2192 jointures LEFT). C\u00f4t\u00e9 NESP, `montant` / `key_factu` / `prod` sont NULL hors \u00e9tats\n  termin\u00e9e sign\u00e9e / signature diff\u00e9r\u00e9e / mise en \u00e9chec ; `delai_*` et\n  `bonus_*` sont NULL hors \u00e9tats termin\u00e9e sign\u00e9e / signature diff\u00e9r\u00e9e\n  (filtre interne \u00e0 `int_nesp_tech__delais_interventions`). Le p\u00e9rim\u00e8tre\n  d'agence, lui, est unique pour toute la cha\u00eene NESP : les 4 agences EVS,\n  filtr\u00e9es au d\u00e9dup (sous-traitant 'nespresso sud' exclu).\n- Plusieurs colonnes sont harmonis\u00e9es mais portent une s\u00e9mantique\n  diff\u00e9rente selon la source \u2014 voir les descriptions de `statut_facturation`,\n  `categorie_machine` / `machine_clean`, `delai_heures_*`, `bonus_bool`,\n  `delai_tech` / `delai_partenaire`, `alias_obj_*`, `client_id`.\n- `type_intervention` : libell\u00e9 en clair align\u00e9 entre sources \u2014 les 4\n  grandes cat\u00e9gories (Curative / Preventive / Installation / Desinstallation)\n  sont comparables NESP\u2194YUMAN. Le regroupement YUMAN est port\u00e9 en amont par\n  `int_yuman__interventions.workorder_type_grouped` ; c\u00f4t\u00e9 NESP le libell\u00e9\n  vient de la facturation (NULL sur les \u00e9tats 'termin\u00e9e non sign\u00e9e').\n- Rafra\u00eechissement : aucun scheduler d\u00e9di\u00e9 \u00e0 la BU technique. Le mart se\n  reconstruit automatiquement via `source:<source>+` quand une source\n  upstream charge. Cadence : voir docs/pipeline-schedule.md.\n"""
    )
    as (
      

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
    );
  