
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions techniques Nespresso (r\u00e9paration, maintenance via Nomad Repair) \u2014 une ligne par intervention, d\u00e9dupliqu\u00e9e. Table de base de toute la cha\u00eene nesp_tech (d\u00e9lais, facturation).\n[COMMENT CONSTRUITE] stg_nesp_tech__interventions d\u00e9dupliqu\u00e9 par n_planning (on conserve la ligne la plus r\u00e9cente : date_heure_fin desc, puis extracted_at desc). Passthrough des colonnes staging. P\u00e9rim\u00e8tre restreint aux 4 agences EVS ('evs' = AURA, 'evs idf', 'evs paris', 'evs paris 2') : 'nespresso sud' est un sous-traitant, pas une agence EVS (643 interventions oct.-d\u00e9c. 2025, 93 k\u20ac, ex\u00e9cut\u00e9es par 7 techniciens absents du r\u00e9f\u00e9rentiel EVS). Filtre en point unique pour toute la cha\u00eene nesp_tech.\n[GRAIN] 1 ligne par n_planning (PK). ~84k lignes.\n[NOTES] Colonnes techniques en fin de table (rn, source_file, extracted_at) = artefacts d'ingestion, sans usage m\u00e9tier.\n"""
    )
    as (
      
-- Liste des interventions dédupliquées par la date de fin, restreinte au
-- périmètre des 4 agences EVS.
--
-- Périmètre : 'nespresso sud' est un SOUS-TRAITANT, pas une agence EVS — ses 643
-- interventions (27/10/2025 → 27/12/2025, flux éteint depuis) sont exécutées par
-- 7 techniciens dont AUCUN n'existe dans le référentiel EVS, là où les 4 agences
-- EVS en résolvent 100 %. Filtré ici, en point unique, plutôt qu'au cas par cas
-- en aval : c'est la divergence de périmètre entre modèles descendants qui a
-- rendu ces lignes incohérentes dans le fait (montant renseigné mais délais,
-- bonus et technicien NULL, `int_nesp_tech__delais_interventions` filtrant quand
-- `int_nesp_tech__facturation_interventions` ne filtrait pas).
-- Les filtres d'agence encore présents en aval (delais, consommation_article,
-- piece_detachee_pricing) sont désormais redondants et cohérents avec celui-ci.
with ranked as (

    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions`
    where agency in ('evs', 'evs idf', 'evs paris', 'evs paris 2')

    qualify ROW_NUMBER() over (
        partition by n_planning
        order by date_heure_fin desc, extracted_at desc
    ) = 1

)

select * from ranked
    );
  