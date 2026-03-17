{{ config(
materialized='table',
cluster_by=['n_serie_machine']
) }}
-- Configuration dbt :
-- materialized='table' : le modèle est matérialisé en table BigQuery
-- cluster_by : clustering sur n_serie_machine pour accélérer les filtres par machine

{% set valid_intervention_states = [
'terminée signée',
'signature différée',
'terminée non signée'
] %}
-- Variable dbt contenant les états d'intervention considérés comme terminés
-- Permet d'éviter de répéter les valeurs dans la requête et facilite la maintenance

with clients as (
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
    from {{ ref('int_nesp_co__clients_enrichis') }}
),
-- CTE CLIENTS
-- Extraction des informations client enrichies
-- Cette table contient les informations de contact et d'adresse utilisées dans le modèle final

interventions_12m as (
    select
        n_client,
        num_serie_machine,
        code_machine,
        n_planning,
        date_heure_fin
    from {{ ref('int_nesp_tech__interventions_dedup') }}
    where
        date_heure_fin >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), interval 12 month))
        and etat_intervention in (
            {% for state in valid_intervention_states %}
                '{{ state }}'{% if not loop.last %},{% endif %}
            {% endfor %}
        )
),
-- CTE INTERVENTIONS_12M
-- Filtre des interventions techniques réalisées sur les 12 derniers mois
-- Le filtre est appliqué directement sur la table source pour réduire le volume de données
-- On conserve uniquement les interventions terminées selon les états définis dans la variable dbt

machines_clean as (
    select
        LOWER(nom_machine) as nom_machine_clean,
        machine_clean
    from {{ ref('ref_nesp_tech__machines_clean') }}
),
-- CTE MACHINES_CLEAN
-- Table de référence permettant de normaliser les noms de machines
-- LOWER est utilisé pour aligner le format avec le code machine provenant des interventions

joined_data as (
    select
        clients.third as mc,
        clients.third_name as nom_client,
        clients.third_secteur as secteur,
        clients.order_placer_name as contact_client,
        clients.order_placer_phone as telephone,
        CONCAT(COALESCE(clients.third_address_1, ''), COALESCE(clients.third_address_2, '')) as adresse,
        clients.third_city as ville,
        clients.third_post_code as code_postal,
        interventions_12m.num_serie_machine as n_serie_machine,
        interventions_12m.code_machine,
        machines_clean.machine_clean as machine,
        interventions_12m.n_planning,
        interventions_12m.date_heure_fin
    from clients
    inner join interventions_12m
        on clients.third = interventions_12m.n_client
    left join machines_clean
        on interventions_12m.code_machine = machines_clean.nom_machine_clean
),
-- CTE JOINED_DATA
-- Jointure principale du modèle
-- 1. Jointure clients → interventions via le code client
-- 2. Jointure interventions → table de référence machines pour obtenir un nom de machine propre
-- Cette étape prépare le dataset avant agrégation

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
        machine,
        COUNT(n_planning) as n_inter_12mois,
        DATE(MAX(date_heure_fin)) as date_last_inter
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
        machine
)

-- CTE AGGREGATED
-- Agrégation des interventions par machine
-- COUNT(n_planning) calcule le nombre d'interventions réalisées sur 12 mois
-- MAX(date_heure_fin) récupère la date de la dernière intervention
select *
from aggregated
qualify
    ROW_NUMBER() over (
        partition by n_serie_machine
        order by date_last_inter desc, RAND()
    ) = 1
-- Déduplication finale
-- On conserve une seule ligne par numéro de série de machine
-- La ligne choisie est celle avec la date de dernière intervention la plus récente
-- Si plusieurs lignes ont la même date, RAND() permet d'en sélectionner une aléatoirement
