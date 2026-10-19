
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__erreur_prevision`
      
    
    cluster by company_id, product_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Backtest \u00ab pr\u00e9vision rejou\u00e9e vs r\u00e9el \u00bb des articles Neshu : pour chaque mois cible pass\u00e9, les trois m\u00e9thodes de pr\u00e9vision rejou\u00e9es sans voir le futur, compar\u00e9es \u00e0 la demande r\u00e9elle.\n[COMMENT CONSTRUITE] int_oracle_neshu__classification_article \u00d7 int_oracle_neshu__demande_mensuelle, mois cibles depuis janvier 2024, couple pris \u00e0 partir de son premier mois de demande.\n[GRAIN] 1 ligne par (mois_cible, company_id, product_id) \u2014 reconstruit int\u00e9gralement \u00e0 chaque build.\n[NOTES] M\u00eame logique que fct_supply_chain__erreur_prevision_neshu, dupliqu\u00e9e volontairement : l'application ne lit pas les marts (cf. docs/conventions/apps.md). Une \u00e9volution de r\u00e8gle se reporte dans les deux. prevision_retenue route selon la classification actuelle.\n"""
    )
    as (
      

-- Copie applicative de fct_supply_chain__erreur_prevision_neshu : même logique, construite sur l'intermediate
-- et le staging pour que l'app Cockpit ne dépende pas des marts (cf. docs/conventions/apps.md).

-- Backtest « prévision rejouée vs réel ».
-- Pour chaque mois cible passé, rejoue les 3 méthodes de prévision telles qu'elles
-- auraient été calculées pendant ce mois (walk-forward : seules les données STRICTEMENT
-- antérieures au mois cible sont visibles — pas de fuite du futur), puis compare au réel.
-- Possible sans ré-entraînement car les méthodes sont des formules déterministes de
-- l'historique : on remplace le current_date() de la prévision (③) par le mois de coupure.
-- Champion-challenger : les 3 candidates sont exposées côte à côte pour arbitrer les
-- évolutions de méthode (sigma d'erreur, Croston, saisonniers) sans re-backtester.
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
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__classification_article`

),

mois_cibles as (

    -- Mois complets rejoués : janv. 2024 (13 mois d'historique amont requis,
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
  