{{ config(
    materialized='table',
    partition_by={'field': 'date_heure_debut', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by=['product_reference']
) }}

-- Pièces Nespresso posées, ligne à ligne, construites depuis l'intermediate nesp_tech et le staging Yuman :
-- jamais chargée en mémoire, interrogée à la demande par l'app (détail d'un mois pour l'export du flux TechCare,
-- conso jour par jour d'une référence sur 12 mois). Interventions réalisées des agences EVS, date renseignée,
-- codes de main-d'œuvre exclus.
with techniciens as (
    -- Technicien Yuman retrouvé par son matricule Nomad.
    select
        user_id,
        nomad_id
    from {{ ref('stg_yuman__users') }}
    where
        nomad_id is not null
        and (user_type = 'technician' or (user_type = 'manager' and is_manager_as_technician = true))
)

select
    i.date_heure_debut,
    a.code_article,
    coalesce(upper(trim(concat('EVS_NESPRESSO_', upper(a.code_article)))), 'NONE') as product_reference,
    t.user_id as technician_id,
    a.nom_article,
    coalesce(a.quantite_article, 0) as qty_consommee
from {{ ref('int_nesp_tech__articles_dedup') }} as a
inner join {{ ref('int_nesp_tech__interventions_dedup') }} as i
    on a.n_planning = i.n_planning
left join techniciens as t
    on lower(a.n_tech) = lower(cast(t.nomad_id as string))
where
    i.agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
    and i.etat_intervention in ('terminée signée', 'terminée non signée')
    and i.date_heure_debut is not null
    and coalesce(upper(trim(a.code_article)), 'NONE') not in ('0000001', '0000100')
