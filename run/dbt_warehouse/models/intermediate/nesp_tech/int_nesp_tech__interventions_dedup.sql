
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions techniques Nespresso (r\u00e9paration, maintenance via Nomad Repair) \u2014 une ligne par intervention, d\u00e9dupliqu\u00e9e. Table de base de toute la cha\u00eene nesp_tech (d\u00e9lais, facturation).\n[COMMENT CONSTRUITE] stg_nesp_tech__interventions d\u00e9dupliqu\u00e9 par n_planning (on conserve la ligne la plus r\u00e9cente : date_heure_fin desc, puis extracted_at desc). Passthrough des colonnes staging.\n[GRAIN] 1 ligne par n_planning (PK). ~84k lignes.\n[NOTES] Colonnes techniques en fin de table (rn, source_file, extracted_at) = artefacts d'ingestion, sans usage m\u00e9tier.\n"""
    )
    as (
      
-- Liste des interventions dédupliquées par la date de fin
with ranked as (

    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions`

    qualify ROW_NUMBER() over (
        partition by n_planning
        order by date_heure_fin desc, extracted_at desc
    ) = 1

)

select * from ranked
    );
  