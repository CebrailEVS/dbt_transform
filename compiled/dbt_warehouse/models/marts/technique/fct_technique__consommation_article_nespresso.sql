

-- Pendant Nespresso (Nomad Repair) de fct_technique__consommation_article_yuman :
-- les interventions Nespresso ne passent pas par les bons Yuman, mais leurs articles
-- sont centralisés dans le référentiel Yuman sous le préfixe EVS_NESPRESSO_.
-- Une ligne de la table articles = une pose réelle (vérifié : 99,95 % des lignes
-- portent un état terminé) — aucun filtre d'état, l'état est exposé en information.

with articles as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup`
),

interventions as (
    -- Contexte d'intervention : états, agence, horodatages début/fin.
    -- Périmètre agences EVS uniquement ('nespresso sud' = sous-traitant,
    -- hors flux de stock EVS, plus actif depuis 2025).
    select
        n_planning,
        etat_intervention,
        agency,
        date_heure_debut,
        date_heure_fin
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
    where agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
),

technicians as (
    -- Rattachement au référentiel technicien Yuman via le matricule Nomad
    -- (nomad_id unique dans la dim, pas de fan-out).
    select
        user_id,
        nomad_id
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
    where nomad_id is not null
)

select
    -- Grain
    a.n_planning,
    a.code_article,

    -- FK dimensions
    t.user_id as technician_id,

    -- Attributs d'affichage
    concat('EVS_NESPRESSO_', upper(a.code_article)) as product_reference,
    a.nom_article,
    a.n_tech,
    a.nom_tech,
    a.prenom_tech,
    a.n_client,
    a.raison_sociale_client,
    a.n_site,
    a.nom_site,
    a.code_machine,
    a.nom_machine,
    a.num_serie_machine,

    -- État métier de l'intervention (information, non filtré)
    i.etat_intervention,
    i.agency,

    -- Dates d'intervention (horodatages iso interventions_dedup)
    i.date_heure_debut,
    i.date_heure_fin,

    -- Mesure
    a.quantite_article as qty_consommee,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '2a4720c1-09c4-4737-acf9-b91f3aad9cdc' as dbt_invocation_id
from articles as a
inner join interventions as i
    on a.n_planning = i.n_planning
left join technicians as t
    on lower(a.n_tech) = lower(cast(t.nomad_id as string))