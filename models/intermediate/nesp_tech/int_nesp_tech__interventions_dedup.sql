{{ config(
    materialized = 'table'
) }}
-- Liste des interventions dédupliquées par la date de fin, restreinte au
-- périmètre des 4 agences EVS.
--
-- Périmètre : 'nespresso sud' est un sous-traitant, pas une agence EVS : filtré
-- ici, en point unique, plutôt qu'au cas par cas en aval. Les filtres d'agence
-- présents en aval (delais, consommation_article, piece_detachee_pricing) sont
-- redondants et cohérents avec celui-ci.
with ranked as (

    select *
    from {{ ref('stg_nesp_tech__interventions') }}
    where agency in ('evs', 'evs idf', 'evs paris', 'evs paris 2')

    qualify row_number() over (
        partition by n_planning
        order by date_heure_fin desc, extracted_at desc
    ) = 1

)

select * from ranked
