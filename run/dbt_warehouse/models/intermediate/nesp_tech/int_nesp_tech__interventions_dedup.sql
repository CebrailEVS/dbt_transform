
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions techniques Nespresso (r\u00e9paration, maintenance via Nomad Repair) \u2014 une ligne par intervention, d\u00e9dupliqu\u00e9e. Table de base de toute la cha\u00eene nesp_tech (d\u00e9lais, facturation).\n[COMMENT CONSTRUITE] stg_nesp_tech__interventions d\u00e9dupliqu\u00e9 par n_planning (on conserve la ligne la plus r\u00e9cente : date_heure_fin desc, puis extracted_at desc). Passthrough des colonnes staging. P\u00e9rim\u00e8tre restreint aux 4 agences EVS ('evs' = AURA, 'evs idf', 'evs paris', 'evs paris 2') : 'nespresso sud' est un sous-traitant, pas une agence EVS. Filtre en point unique pour toute la cha\u00eene nesp_tech.\n[GRAIN] 1 ligne par n_planning (PK).\n[NOTES] Colonnes techniques en fin de table (rn, source_file, extracted_at) = artefacts d'ingestion, sans usage m\u00e9tier.\n"""
    )
    as (
      
-- Liste des interventions dédupliquées par la date de fin, restreinte au
-- périmètre des 4 agences EVS.
--
-- Périmètre : 'nespresso sud' est un sous-traitant, pas une agence EVS : filtré
-- ici, en point unique, plutôt qu'au cas par cas en aval. Les filtres d'agence
-- présents en aval (delais, consommation_article, piece_detachee_pricing) sont
-- redondants et cohérents avec celui-ci.
with ranked as (

    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions`
    where agency in ('evs', 'evs idf', 'evs paris', 'evs paris 2')

    qualify row_number() over (
        partition by n_planning
        order by date_heure_fin desc, extracted_at desc
    ) = 1

)

select * from ranked
    );
  