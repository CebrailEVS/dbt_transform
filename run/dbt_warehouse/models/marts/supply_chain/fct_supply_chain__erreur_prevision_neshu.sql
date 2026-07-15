
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`
      
    
    cluster by company_id, product_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Backtest de la pr\u00e9vision de demande Neshu : pour chaque mois pass\u00e9, la pr\u00e9vision telle qu'elle aurait \u00e9t\u00e9 calcul\u00e9e pendant ce mois (avec uniquement les donn\u00e9es ant\u00e9rieures \u2014 walk-forward, pas de fuite du futur) face \u00e0 la demande r\u00e9elle. Fondation V2 : mesure l'erreur et le biais du forecast article par article, et arbitre les \u00e9volutions de m\u00e9thode (champion-challenger). En V2.1, l'\u00e9cart-type de l'erreur remplacera l'\u00e9cart-type de la demande brute dans le stock de s\u00e9curit\u00e9 du point de commande.\n[COMMENT CONSTRUITE] Rejeu d\u00e9terministe des formules de fct_supply_chain__prevision_demande_neshu sur le socle int_oracle_neshu__demande_mensuelle, en rempla\u00e7ant current_date() par chaque mois de coupure : prevision_moyenne_mobile = somme des N mois complets pr\u00e9c\u00e9dant le mois cible / N (var prevision_nb_mois_moyenne, d\u00e9faut 3) ; prevision_saisonniere = demande N-1 sur la fen\u00eatre [M-12, M-10] / 3 (fid\u00e8le \u00e0 \u2462, fen\u00eatre telle que vue pendant le mois cible) ; prevision_naive = demande de la fen\u00eatre / nb de mois avec demande. prevision_retenue = la candidate rout\u00e9e par methode_prevision de fct_supply_chain__classification_article_neshu (classification ACTUELLE, non rejou\u00e9e \u00e0 date \u2014 limite assum\u00e9e, cf. NOTES). erreur = demande_reelle \u2212 prevision_retenue (positive = sous-pr\u00e9vision). Mois cibles : janv. 2024 \u2192 dernier mois clos (g\u00e9n\u00e9r\u00e9 \u00e0 chaque build : le mois qui vient de se clore entre automatiquement). Un couple n'entre qu'\u00e0 partir de son premier mois de demande.\n[GRAIN] 1 ligne par (mois_cible, company_id, product_id) \u2014 full rebuild quotidien, ~10 000 lignes.\n[NOTES] Les 3 m\u00e9thodes candidates sont expos\u00e9es c\u00f4te \u00e0 c\u00f4te (champion-challenger) : comparer les colonnes suffit \u00e0 arbitrer V2.1-V2.4 sans re-backtester. Le routage utilise la classification d'aujourd'hui, pas celle qu'on aurait eue \u00e0 chaque date (rejouer Syntetos-Boylan/ABC \u00e0 date = co\u00fbt \u00e9lev\u00e9, gain faible ; la vraie classif \u00e0 date s'accumulera via le snapshot mensuel de la classification, volet s\u00e9par\u00e9). Pas de MAPE au grain ligne (demande sparse \u2192 division par z\u00e9ro) : erreurs en unit\u00e9s, WMAPE \u00e0 calculer en agr\u00e9g\u00e9. Filtrer nb_mois_anciennete >= 3 pour \u00e9carter les premiers mois de vie (fen\u00eatre incompl\u00e8te). date_calcul en fuseau Europe/Paris (contrairement aux marts V1, encore en UTC).\n"""
    )
    as (
      

-- Backtest « prévision rejouée vs réel » (V2.0 — fondation de la feuille de route V2).
-- Pour chaque mois cible passé, rejoue les 3 méthodes de prévision V1 telles qu'elles
-- auraient été calculées pendant ce mois (walk-forward : seules les données STRICTEMENT
-- antérieures au mois cible sont visibles — pas de fuite du futur), puis compare au réel.
-- Possible sans ré-entraînement car les méthodes sont des formules déterministes de
-- l'historique : on remplace le current_date() de la prévision (③) par le mois de coupure.
-- Champion-challenger : les 3 candidates sont exposées côte à côte pour arbitrer les
-- évolutions V2.1+ (sigma d'erreur, Croston, saisonniers) sans re-backtester.
-- Limite assumée : prevision_retenue route selon la classification ACTUELLE (②), pas
-- celle rejouée à chaque date (la classif à date s'accumulera via le snapshot mensuel).

with classification as (

    select
        company_id,
        product_id,
        company_code,
        company_name,
        product_code,
        product_name,
        statut_vie,
        classe_abc,
        methode_prevision
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`

),

mois_cibles as (

    -- Mois complets rejoués : janv. 2024 (le socle démarre déc. 2022 -> 13 mois d'amont,
    -- la référence saisonnière N-1 est toujours couverte) jusqu'au dernier mois clos.
    -- Le mois qui vient de se clore entre automatiquement au build suivant.
    select mois_cible
    from unnest(generate_date_array(
        date '2024-01-01',
        date_sub(date_trunc(current_date('Europe/Paris'), month), interval 1 month),
        interval 1 month
    )) as mois_cible

),

premiere_demande as (

    select
        company_id,
        product_id,
        min(demande_mois) as premiere_demande_mois
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle`
    group by company_id, product_id

),

grille as (

    -- Un couple n'entre dans le backtest qu'à partir de son premier mois de demande
    -- (avant : prévision 0 vs réel 0 = lignes vides qui flatteraient les erreurs).
    select
        c.company_id,
        c.product_id,
        c.company_code,
        c.company_name,
        c.product_code,
        c.product_name,
        c.statut_vie,
        c.classe_abc,
        c.methode_prevision,
        m.mois_cible,
        date_diff(m.mois_cible, p.premiere_demande_mois, month) as nb_mois_anciennete
    from classification as c
    inner join premiere_demande as p
        on c.company_id = p.company_id and c.product_id = p.product_id
    inner join mois_cibles as m
        on p.premiere_demande_mois <= m.mois_cible

),

retropolation as (

    -- Rejoue, pour chaque (couple, mois cible), les agrégats que la prévision (③) aurait
    -- calculés pendant ce mois : fenêtre = N mois complets avant le mois cible ;
    -- saison = fenêtre N-1 telle que vue pendant le mois cible ([M-12, M-10], fidèle à ③).
    select
        g.company_id,
        g.product_id,
        g.mois_cible,
        coalesce(sum(
            case
                when
                    s.demande_mois >= date_sub(
                        g.mois_cible, interval 3 month
                    )
                    and s.demande_mois < g.mois_cible
                    then s.quantite_demandee
            end
        ), 0) as demande_fenetre,
        count(
            case
                when
                    s.demande_mois >= date_sub(
                        g.mois_cible, interval 3 month
                    )
                    and s.demande_mois < g.mois_cible
                    then s.demande_mois
            end
        ) as nb_mois_avec_demande,
        coalesce(sum(
            case
                when
                    s.demande_mois between date_sub(g.mois_cible, interval 12 month)
                    and date_sub(g.mois_cible, interval 10 month)
                    then s.quantite_demandee
            end
        ), 0) as demande_saison_n1_totale,
        coalesce(sum(
            case when s.demande_mois = g.mois_cible then s.quantite_demandee end
        ), 0) as demande_reelle
    from grille as g
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle` as s
        on
            g.company_id = s.company_id
            and g.product_id = s.product_id
            and s.demande_mois between date_sub(g.mois_cible, interval 12 month) and g.mois_cible
    group by g.company_id, g.product_id, g.mois_cible

),

previsions as (

    select
        g.mois_cible,
        g.company_id,
        g.product_id,
        g.company_code,
        g.company_name,
        g.product_code,
        g.product_name,
        g.statut_vie,
        g.classe_abc,
        g.methode_prevision,
        g.nb_mois_anciennete,
        r.demande_reelle,
        r.nb_mois_avec_demande,
        round(r.demande_fenetre / 3, 2)
            as prevision_moyenne_mobile,
        round(r.demande_saison_n1_totale / 3, 2) as prevision_saisonniere,
        round(
            safe_divide(r.demande_fenetre, greatest(r.nb_mois_avec_demande, 1)), 2
        ) as prevision_naive
    from grille as g
    inner join retropolation as r
        on
            g.company_id = r.company_id
            and g.product_id = r.product_id
            and g.mois_cible = r.mois_cible

),

final as (

    select
        *,
        case
            when methode_prevision = 'exclu' then 0
            when methode_prevision = 'reference_saisonniere' then prevision_saisonniere
            when methode_prevision = 'naif' then prevision_naive
            else prevision_moyenne_mobile
        end as prevision_retenue
    from previsions

)

select
    mois_cible,
    company_id,
    product_id,
    company_code,
    company_name,
    product_code,
    product_name,
    statut_vie,
    classe_abc,
    methode_prevision,
    nb_mois_anciennete,
    nb_mois_avec_demande,
    demande_reelle,
    prevision_moyenne_mobile,
    prevision_saisonniere,
    prevision_naive,
    prevision_retenue,
    round(demande_reelle - prevision_retenue, 2) as erreur,
    round(abs(demande_reelle - prevision_retenue), 2) as erreur_absolue,
    current_date('Europe/Paris') as date_calcul
from final
    );
  