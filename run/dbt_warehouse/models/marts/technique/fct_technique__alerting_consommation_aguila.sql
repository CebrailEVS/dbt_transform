
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__alerting_consommation_aguila`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nContr\u00f4le qualit\u00e9 des consommations de pi\u00e8ces lors des interventions\ntechniques Nespresso sur machines Aguila. D\u00e9tecte les incoh\u00e9rences\n(maintenance pr\u00e9ventive, kits, filtres, pi\u00e8ces hors kit) via des r\u00e8gles\nm\u00e9tier d'alerting.\n\n[COMMENT CONSTRUITE]\nLecture de `int_nesp_tech__alerte_conso_aguila`, qui consolide les\nconsommations d'articles par intervention et applique les r\u00e8gles m\u00e9tier\nd'alerting (un flag par type d'incoh\u00e9rence d\u00e9tectable). Le mart y ajoute les\ncolonnes de tra\u00e7abilit\u00e9 dbt.\n\n[GRAIN]\n1 ligne par intervention Aguila.\n\n[NOTES]\nAguila = type de machine Nespresso. Le mart est consomm\u00e9 par le BI\nPILOTAGE TECHNIQUE pour les alertes op\u00e9rationnelles techniciens.\n"""
    )
    as (
      

select
    -- Information sur l'intervention
    n_planning,
    date_fin,
    intervention_type,
    consignes,
    categorie_machine,
    machine_clean,

    -- Flags Alertes Conso
    alt_nomenclature,
    alt_coherence_consigne,
    alt_prevcompletekit,
    alt_kit_typemachine,
    alt_filtre_consomme,
    alt_nb_kits,
    alt_conso_hors_kit,

    -- Compilation des alertes
    alerte_compile,

    -- Métadonnées dbt (audit & lineage)
    current_timestamp() as dbt_updated_at,
    '01a1f932-ffca-74e0-92e5-fff599200ce2' as dbt_invocation_id  -- noqa: TMP

from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__alerte_conso_aguila`
    );
  