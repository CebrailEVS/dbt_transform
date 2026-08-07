
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