{{
    config(
        materialized='table',
        cluster_by=['company_id', 'product_id']
    )
}}

-- Prévision de demande NESHU par dépôt et article (maillon 3 : le forecast lui-même).
-- Applique la méthode routée par la classification (② l'aiguilleur) :
--   moyenne_mobile / moyenne_mobile_surveille -> moyenne des N derniers mois COMPLETS ;
--   reference_saisonniere (saisonniers avec historique) -> demande du mois À VENIR l'an dernier
--     (fenêtre N-1 ± 1 mois) : anticipe le redémarrage de saison là où la MA récente sous-estime
--     (ex. barres d'hiver ≈ 0 en été → la MA prédit ~0 pour septembre alors que le N-1 donne le
--     vrai niveau de reprise) ;
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

demande_n1_saison as (

    -- Référence saisonnière : demande du mois À VENIR l'an dernier, fenêtre ± 1 mois (÷ 3 pour lisser).
    -- Cible = mois suivant ; centre N-1 = ce mois il y a 1 an. Pour juillet : fenêtre juil→sept N-1,
    -- centrée sur août N-1. Anticipe le redémarrage (la MA récente, hors-saison ≈ 0, ne le voit pas).
    select
        c.company_id,
        c.product_id,
        coalesce(sum(s.quantite_demandee), 0) / 3 as demande_saison_n1
    from classification as c
    left join {{ ref('int_oracle_neshu__demande_mensuelle') }} as s
        on
            c.company_id = s.company_id
            and c.product_id = s.product_id
            and s.demande_mois between
            date_sub(date_trunc(current_date(), month), interval 12 month)
            and date_sub(date_trunc(current_date(), month), interval 10 month)
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
            when c.methode_prevision = 'reference_saisonniere' then round(n1.demande_saison_n1, 2)
            when c.methode_prevision = 'naif'
                then round(safe_divide(f.demande_fenetre, greatest(f.nb_mois_avec_demande, 1)), 2)
            else round(f.demande_fenetre / {{ var('prevision_nb_mois_moyenne', 3) }}, 2)
        end as demande_prevue_mensuelle
    from classification as c
    inner join demande_fenetre as f
        on c.company_id = f.company_id and c.product_id = f.product_id
    inner join demande_n1_saison as n1
        on c.company_id = n1.company_id and c.product_id = n1.product_id

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
