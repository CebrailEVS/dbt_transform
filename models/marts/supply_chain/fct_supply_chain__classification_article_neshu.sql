{{
    config(
        materialized='table',
        cluster_by=['company_id', 'product_id']
    )
}}

-- Classification article × dépôt pour le forecast/réappro NESHU : l'"aiguilleur".
-- Range chaque couple (dépôt, article) selon 3 axes, puis en déduit la méthode de prévision.
-- N'émet AUCUN forecast (ça, c'est le maillon suivant) :
--   - comportement de demande : Syntetos-Boylan (ADI / CV²) sur la vie active ;
--   - cycle de vie : label product_exploit (autorité) + comportement (filet de sécurité) ;
--   - importance : ABC en VALEUR (€), Pareto par dépôt.
-- Périmètre : 5 dépôts de distribution produits (hors ateliers / périmés / rebus / stock Nunshen
-- et entités non-dépôt), articles Neshu, hors familles non-consommables (PRESENTOIR, PRESTATION
-- SERVICE) et hors BADGENESHU. Représentation : 1 ligne par (dépôt, article) — état courant.

with demande as (

    select
        company_id,
        product_id,
        product_code,
        demande_mois,
        quantite_demandee
    from {{ ref('int_oracle_neshu__demande_mensuelle') }}

),

stats as (

    -- Statistiques de demande par couple, sur la vie active (min→max des mois observés).
    -- Pas de densification : l'ADI vient de l'écart de dates, le CV² des mois non nuls.
    select
        company_id,
        product_id,
        product_code,
        min(demande_mois) as premiere_sortie,
        max(demande_mois) as derniere_sortie,
        count(*) as nb_mois_demande,
        sum(quantite_demandee) as demande_totale,
        sum(
            if(
                demande_mois >= date_sub(date_trunc(current_date(), month), interval 12 month),
                quantite_demandee, 0
            )
        ) as demande_12m,
        date_diff(max(demande_mois), min(demande_mois), month) + 1 as span_actif_mois,
        date_diff(date_trunc(current_date(), month), max(demande_mois), month) as mois_inactif,
        round(safe_divide(date_diff(max(demande_mois), min(demande_mois), month) + 1, count(*)), 2) as adi,
        round(pow(safe_divide(stddev_samp(quantite_demandee), avg(quantite_demandee)), 2), 2) as cv2
    from demande
    group by company_id, product_id, product_code

),

depots as (

    -- Les 5 dépôts de distribution produits uniquement. Exclut ateliers, périmés, rebus, stock
    -- Nunshen, et les entités non-dépôt (fournisseur, client, logistique) qui polluent l'historique
    -- source. Aligné avec le périmètre du mart de couverture.
    select company_id
    from {{ ref('dim_neshu__company') }}
    where company_code in (
        'DEPOTRUNGIS', 'DEPOTLYON', 'DEPOTBORDEAUX', 'DEPOTSTRASBOURG', 'DEPOTMARSEILLE'
    )

),

produit as (

    -- Périmètre (dépôts + articles) + enrichissement produit (label exploit, prix pour l'ABC valeur).
    select
        s.*,
        p.product_name,
        p.product_exploit,
        s.demande_12m * coalesce(p.purchase_unit_price, 0) as valeur_12m
    from stats as s
    inner join depots as d on s.company_id = d.company_id
    inner join {{ ref('dim_neshu__product') }} as p on s.product_id = p.product_id
    where
        p.product_owner = 'Neshu'
        and (p.product_family is null or p.product_family not in ('PRESENTOIR', 'PRESTATION SERVICE'))
        and s.product_code != 'BADGENESHU'

),

abc as (

    -- ABC en valeur : Pareto cumulé par dépôt (sur les couples à demande récente > 0).
    select
        *,
        case
            when demande_12m > 0 then
                100 * sum(valeur_12m) over (
                    partition by company_id order by valeur_12m desc
                    rows between unbounded preceding and current row
                ) / nullif(sum(valeur_12m) over (partition by company_id), 0)
        end as cumul_valeur_pct,
        -- Récence de l'article TOUS dépôts confondus (le label exploit est article-level).
        min(mois_inactif) over (partition by product_id) as mois_inactif_article
    from produit

),

classifie as (

    select
        *,
        -- Statut = ACTION (prévoir / exclure), au grain couple : un article qui vend récemment à
        -- ce dépôt est 'actif' quel que soit son label (le réel gagne, jamais de rupture manquée).
        -- L'incohérence de label est portée à part, au grain article, par alerte_label.
        case
            when mois_inactif < 4 then 'actif'
            when product_exploit = 'NON' then 'arrete'
            else 'inactif'
        end as statut_vie,
        -- Alerte d'incohérence label ↔ réel, au grain ARTICLE (le label exploit est article-level) :
        -- évite les faux positifs d'un article actif ailleurs mais silencieux à ce dépôt.
        case
            when product_exploit = 'NON' and mois_inactif_article < 4 then 'arrete_mais_vend'
            when product_exploit = 'OUI' and mois_inactif_article >= 4 then 'actif_mais_silencieux'
        end as alerte_label,
        -- Comportement de demande (Syntetos-Boylan) ; indetermine si trop peu d'historique.
        case
            when nb_mois_demande < 3 then 'indetermine'
            when adi < 1.32 and cv2 < 0.49 then 'regulier'
            when adi < 1.32 then 'erratique'
            when cv2 < 0.49 then 'intermittent'
            else 'lumpy'
        end as classe_demande,
        -- Importance ABC en valeur (A ≤ 80 % / B ≤ 95 % / C) ; null si pas de demande récente.
        case
            when demande_12m = 0 then null
            when cumul_valeur_pct <= 80 then 'A'
            when cumul_valeur_pct <= 95 then 'B'
            else 'C'
        end as classe_abc
    from abc

)

select
    current_date() as date_calcul,
    company_id,
    product_id,
    product_code,
    product_name,
    statut_vie,
    classe_demande,
    classe_abc,
    -- Méthode V1 : moyenne mobile pour l'essentiel ; Croston (intermittent/lumpy) = V2.
    case
        when statut_vie in ('arrete', 'inactif') then 'exclu'
        when classe_demande = 'indetermine' then 'naif'
        when classe_demande in ('intermittent', 'lumpy') then 'moyenne_mobile_surveille'
        else 'moyenne_mobile'
    end as methode_prevision,
    alerte_label,
    premiere_sortie,
    derniere_sortie,
    mois_inactif,
    nb_mois_demande,
    adi,
    cv2,
    demande_totale,
    demande_12m,
    round(valeur_12m) as valeur_12m
from classifie
