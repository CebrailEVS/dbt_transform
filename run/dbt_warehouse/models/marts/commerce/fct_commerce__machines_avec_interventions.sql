
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_commerce__machines_avec_interventions`
      
    
    

    
    OPTIONS(
      description="""Vue d\u00e9dupliqu\u00e9e des machines actives sur les 6 derniers mois, enrichie des informations client et du libell\u00e9 machine normalis\u00e9. Granularit\u00e9 : 1 ligne par num\u00e9ro de s\u00e9rie de machine. En cas de machine pr\u00e9sente chez plusieurs clients, on conserve la ligne associ\u00e9e \u00e0 la derni\u00e8re intervention (puis code_machine ASC, puis mc ASC pour un arbitrage d\u00e9terministe).\n"""
    )
    as (
      

/*
==============================================================================
MODÈLE : mart_nesp_tech__machines_actives
==============================================================================
Objectif :
    Fournir une vue dédupliquée par numéro de série de machine, enrichie des
    informations client et du libellé machine normalisé, à partir des
    interventions clôturées des 6 derniers mois.

Granularité : 1 ligne par numéro de série (n_serie_machine)
Déduplication : pour un même n_serie_machine, on conserve la ligne dont la
    dernière intervention est la plus récente. En cas d'égalité, on privilégie
    le code_machine le plus petit, puis le mc le plus petit.

Sources :
    - int_nesp_tech__interventions_dedup  → interventions techniques
    - int_nesp_co__clients_enrichis       → référentiel clients
    - ref_nesp_tech__machines_clean       → normalisation des noms de machines
==============================================================================
*/


-- =============================================================================
-- CTE 1 : INTERVENTIONS
-- =============================================================================
-- Filtre en amont sur les 6 derniers mois et les états d'intervention valides
-- (terminée signée, signature différée, terminée non signée).
-- La comparaison porte sur DATE(date_heure_fin) pour favoriser l'utilisation
-- d'éventuelles partitions ou clusterings sur la colonne date.
-- Seules les colonnes nécessaires aux jointures et agrégations sont projetées.
-- =============================================================================
with interventions as (
    select
        n_client,
        num_serie_machine,
        code_machine,
        nom_machine,
        n_planning,
        date_heure_fin
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
    where
        DATE(date_heure_fin) >= DATE_SUB(CURRENT_DATE(), interval 6 month)
        and etat_intervention in (
            'terminée signée',
            'signature différée',
            'terminée non signée'
        )
),

-- =============================================================================
-- CTE 2 : CLIENTS
-- =============================================================================
-- Projection minimale sur le référentiel clients enrichis.
-- Contient les informations d'identification, de contact et d'adresse du client.
-- =============================================================================
clients as (
    select
        third,
        third_name,
        third_secteur,
        order_placer_name,
        order_placer_phone,
        third_address_1,
        third_address_2,
        third_city,
        third_post_code
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__clients_enrichis`
),

-- =============================================================================
-- CTE 3 : MACHINES_CLEAN
-- =============================================================================
-- Table de référence permettant de normaliser les noms de machines bruts vers
-- un libellé harmonisé (machine_clean). La jointure s'effectuera en LOWER()
-- pour s'affranchir des variations de casse.
-- =============================================================================
machines_clean as (
    select
        LOWER(nom_machine) as nom_machine_clean,
        machine_clean
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
),

-- =============================================================================
-- CTE 4 : JOINED_DATA
-- =============================================================================
-- Jointure centrale après réduction du volume :
--   - INNER JOIN clients  : on ne conserve que les interventions dont le client
--     est connu dans le référentiel (cohérence métier).
--   - LEFT JOIN machines_clean : la normalisation du nom machine est optionnelle ;
--     si aucun libellé normalisé n'existe, machine_clean sera NULL.
-- L'adresse est construite par concaténation des deux lignes d'adresse,
-- en gérant les NULL via COALESCE.
-- =============================================================================
joined_data as (
    select
        -- Identifiant client
        i.n_client as mc,

        -- Informations client
        c.third_name as nom_client,
        c.third_secteur as secteur,
        c.order_placer_name as contact_client,
        c.order_placer_phone as telephone,
        CONCAT(COALESCE(c.third_address_1, ''), COALESCE(c.third_address_2, ''))
            as adresse,
        c.third_city as ville,
        c.third_post_code as code_postal,

        -- Informations machine
        i.num_serie_machine as n_serie_machine,
        i.code_machine,
        i.nom_machine,
        m.machine_clean as machine,

        -- Informations intervention
        i.n_planning,
        i.date_heure_fin

    from interventions as i
    inner join clients as c
        on i.n_client = c.third
    left join machines_clean as m
        on LOWER(i.nom_machine) = m.nom_machine_clean
),

-- =============================================================================
-- CTE 5 : AGGREGATED
-- =============================================================================
-- Agrégation par grain (n_serie_machine + contexte client + machine) :
--   - n_inter_6mois  : nombre d'interventions sur les 6 derniers mois
--   - date_last_inter: date de la dernière intervention (MAX de date_heure_fin)
-- Le GROUP BY inclut toutes les dimensions descriptives pour éviter toute perte
-- d'information lors de l'agrégation.
-- =============================================================================
aggregated as (
    select
        mc,
        nom_client,
        secteur,
        contact_client,
        telephone,
        adresse,
        ville,
        code_postal,
        n_serie_machine,
        code_machine,
        nom_machine,
        machine,
        COUNT(*) as n_inter_6mois,
        MAX(date_heure_fin) as date_last_inter
    from joined_data
    group by
        mc,
        nom_client,
        secteur,
        contact_client,
        telephone,
        adresse,
        ville,
        code_postal,
        n_serie_machine,
        code_machine,
        nom_machine,
        machine
)

-- =============================================================================
-- SELECT FINAL
-- =============================================================================
-- Déduplication déterministe par numéro de série (QUALIFY + ROW_NUMBER) :
--   Priorité 1 : intervention la plus récente (date_last_inter DESC)
--   Priorité 2 : code_machine le plus petit (ASC) — arbitrage stable
--   Priorité 3 : mc le plus petit (ASC) — arbitrage stable
--
-- Colonnes exposées, regroupées par thématique :
--   [A] Identification client
--   [B] Coordonnées & contact client
--   [C] Localisation client
--   [D] Identification machine
--   [E] Métriques d'activité
-- =============================================================================
select
    -- -------------------------------------------------------------------------
    -- [A] Identification client
    -- -------------------------------------------------------------------------
    mc,
    nom_client,
    secteur,

    -- -------------------------------------------------------------------------
    -- [B] Coordonnées & contact client
    -- -------------------------------------------------------------------------
    contact_client,
    telephone,

    -- -------------------------------------------------------------------------
    -- [C] Localisation client
    -- -------------------------------------------------------------------------
    adresse,
    ville,
    code_postal,

    -- -------------------------------------------------------------------------
    -- [D] Identification machine
    -- -------------------------------------------------------------------------
    n_serie_machine,
    code_machine,
    nom_machine,
    machine,          -- libellé normalisé (peut être NULL si hors référentiel)

    -- -------------------------------------------------------------------------
    -- [E] Métriques d'activité (fenêtre glissante 6 mois)
    -- -------------------------------------------------------------------------
    n_inter_6mois,
    date_last_inter

from aggregated
qualify ROW_NUMBER() over (
    partition by n_serie_machine
    order by
        date_last_inter desc,
        code_machine asc,
        mc asc
) = 1
    );
  