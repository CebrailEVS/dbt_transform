

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
    '01a1f933-b923-7692-ab5a-5f1471cd5877' as dbt_invocation_id  -- noqa: TMP

from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__alerte_conso_aguila`