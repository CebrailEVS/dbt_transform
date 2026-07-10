{{
    config(
        materialized='table',
        cluster_by=['company_id', 'product_id']
    )
}}

-- Prévision de demande NESHU par dépôt et article (maillon 3 : le forecast lui-même).
-- Applique la méthode routée par la classification (② l'aiguilleur) :
--   moyenne_mobile / moyenne_mobile_surveille -> moyenne des N derniers mois COMPLETS ;
--   naif (indetermine) -> moyenne des mois disponibles ; exclu (arrete/inactif) -> 0.
-- Une moyenne mobile = somme(fenêtre) / N : les mois sans demande comptent comme 0 (implicite),
-- donc pas de densification. Le mois courant, incomplet, est EXCLU (sinon sous-estimation).
-- Fenêtre paramétrable via var dbt prevision_nb_mois_moyenne (défaut 3).

with classification as (

    select
        company_id,
        product_id,
        product_code,
        product_name,
        statut_vie,
        classe_abc,
        methode_prevision
    from {{ ref('fct_supply_chain__classification_article_neshu') }}

),

demande_fenetre as (

    -- Somme de la demande sur les N derniers mois COMPLETS (mois courant exclu),
    -- rattachée à chaque couple de la classification (0 si aucune demande sur la fenêtre).
    select
        c.company_id,
        c.product_id,
        coalesce(sum(s.quantite_demandee), 0) as demande_fenetre,
        count(s.demande_mois) as nb_mois_avec_demande
    from classification as c
    left join {{ ref('int_oracle_neshu__demande_mensuelle') }} as s
        on
            c.company_id = s.company_id
            and c.product_id = s.product_id
            and s.demande_mois >= date_sub(
                date_trunc(current_date(), month), interval {{ var('prevision_nb_mois_moyenne', 3) }} month
            )
            and s.demande_mois < date_trunc(current_date(), month)
    group by c.company_id, c.product_id

),

prevision as (

    select
        c.company_id,
        c.product_id,
        c.product_code,
        c.product_name,
        c.statut_vie,
        c.classe_abc,
        c.methode_prevision,
        f.demande_fenetre,
        f.nb_mois_avec_demande,
        case
            when c.methode_prevision = 'exclu' then 0
            when c.methode_prevision = 'naif'
                then round(safe_divide(f.demande_fenetre, greatest(f.nb_mois_avec_demande, 1)), 2)
            else round(f.demande_fenetre / {{ var('prevision_nb_mois_moyenne', 3) }}, 2)
        end as demande_prevue_mensuelle
    from classification as c
    inner join demande_fenetre as f
        on c.company_id = f.company_id and c.product_id = f.product_id

)

select
    current_date() as date_calcul,
    company_id,
    product_id,
    product_code,
    product_name,
    statut_vie,
    classe_abc,
    methode_prevision,
    demande_fenetre,
    nb_mois_avec_demande,
    demande_prevue_mensuelle,
    round(demande_prevue_mensuelle / 30.4, 3) as demande_prevue_journaliere
from prevision
