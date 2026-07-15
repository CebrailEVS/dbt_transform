

-- Point de commande NESHU par dépôt et article (maillon 4 : la suggestion de réappro).
-- Assemble la prévision (③), le stock actuel, le délai fournisseur, les commandes en cours et un
-- stock de sécurité calibré par classe ABC, puis émet une quantité à commander + un feu tricolore.
-- Construit EN PARALLÈLE de fct_supply_chain__couverture_stock_neshu (bascule PBI après comparaison).
--
-- Méthode de sécurité = V2 (validée par backtest, cf. .claude/notes/supply_chain/forecast_v2) :
-- la sécurité couvre « la demande bouge PLUS QUE PRÉVU » -> on dimensionne sur l'ERREUR de
-- prévision (mesurée par le backtest fct_supply_chain__erreur_prevision_neshu), pas sur la
-- variabilité brute de la demande. La calibration a prouvé que cette méthode TIENT les cibles
-- de service (A 98 / B 95 / C 90 %) là où σ(demande) sous-servait de 3-4 pts.
--
-- Formules :
--   position_stock  = stock_actuel + encours_fournisseur
--   delai_jours     = délai fournisseur moyen PAR ARTICLE (réceptions 12 mois), fallback médiane globale
--   horizon_jours   = delai_jours + revue_jours (période entre 2 passations de commande, var, défaut 0)
--   source_sigma    = 'erreur' si >= sigma_erreur_min_mois (var, défaut 6) mois d'erreur backtestés,
--                     sinon 'demande_fallback' (jamais dé-protégé faute de données)
--   sigma_retenu    = σ(erreur) si source='erreur', sinon σ(demande)
--   sigma_lead_time = sigma_retenu × √(horizon / 30.4)  -- variabilité ramenée à l'horizon couvert
--   stock_securite  = z(niveau de service ABC) × sigma_lead_time  -- z : A=2,05 / B=1,645 / C=1,28
--   demande_retenue = prévision ③ + correction de biais POSITIF (sous-prévision -> rupture) si
--                     source='erreur' ; le biais négatif (sur-prévision) n'est PAS retranché (prudence)
--   point_commande  = demande_retenue_journaliere × horizon + stock_securite
--   qte_a_commander = greatest(0, point_commande − position_stock)
--   statut          = rupture (position ≤ sécurité) / a_commander (≤ point de commande) / ok / exclu
--
-- Grain : 1 ligne par (dépôt, article), aligné sur la classification ② et la prévision ③.
-- Périmètre encours : commande fournisseur FAIT/VALIDE + livraison EN_ATTENTE, < 60 j (règle métier, var).
-- NOTE : chaque dépôt principal (Rungis, Lyon, Marseille) gère son propre stock et passe ses propres
-- commandes fournisseur, rattachées par product_destination_id. Bordeaux/Strasbourg : peu ou pas de
-- commandes directes (à confirmer avec le métier).
-- Paramètres métier en var() dbt : z_service_a/b/c, encours_max_jours, revue_jours, sigma_erreur_min_mois.

with prevision as (

    select
        company_id,
        product_id,
        company_code,
        company_name,
        product_code,
        product_name,
        statut_vie,
        classe_abc,
        methode_prevision,
        demande_prevue_mensuelle,
        demande_prevue_journaliere
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__prevision_demande_neshu`

),

variabilite as (

    -- Écart-type de la demande mensuelle par couple, sur la vie active (mois observés, sparse).
    -- Sert de FALLBACK quand le backtest n'a pas assez d'historique d'erreur (< sigma_erreur_min_mois).
    -- null si < 2 mois de demande -> ramené à 0 (pas de sécurité calculable).
    select
        company_id,
        product_id,
        coalesce(stddev_samp(quantite_demandee), 0) as sigma_demande_mensuelle
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle`
    group by company_id, product_id

),

erreur_prevision as (

    -- σ et biais de l'ERREUR de prévision par couple, mesurés par le backtest walk-forward
    -- (fait agrégé à un grain plus grossier, autorisé). Fenêtre : 12 derniers mois cibles,
    -- vie >= 3 mois (fenêtres de prévision complètes). erreur = réel − prévision (positif = sous-prévision).
    select
        company_id,
        product_id,
        count(*) as nb_mois_erreur,
        stddev_samp(erreur) as sigma_erreur_mensuelle,
        avg(erreur) as biais_mensuel
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`
    where
        nb_mois_anciennete >= 3
        and mois_cible >= date_sub(date_trunc(current_date('Europe/Paris'), month), interval 12 month)
    group by company_id, product_id

),

delai_article as (

    -- Délai fournisseur moyen par article (réceptions valides des 12 derniers mois).
    select
        product_id,
        avg(delivery_lead_time_days) as delai_jours_article
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__reception_tasks`
    where
        is_lead_time_valid
        and date(task_start_date) >= date_sub(current_date(), interval 365 day)
    group by product_id

),

delai_global as (

    -- Fallback : délai médian tous articles confondus, pour un article sans réception récente.
    select approx_quantiles(delivery_lead_time_days, 100)[offset(50)] as delai_jours_global
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__reception_tasks`
    where
        is_lead_time_valid
        and date(task_start_date) >= date_sub(current_date(), interval 365 day)

),

stock_actuel as (

    -- Dernier stock théorique connu par dépôt × article (5 dépôts de distribution uniquement :
    -- exclut périmés 251, rebus 244 et véhicules de tournée). Le mart stock est un historique
    -- quotidien -> on garde le snapshot le plus récent par couple.
    select
        id_entity as company_id,
        product_code,
        stock_at_date as stock_actuel
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
    where
        entity_type = 'company'
        and id_entity in (114, 116, 120, 118, 364)
    qualify row_number() over (partition by id_entity, product_code order by date_system desc) = 1

),

encours as (

    -- Commandes fournisseur en cours (émises, pas encore reçues) à destination du dépôt.
    -- Rattachement par product_destination_id (= dépôt destinataire, type COMPANY), pas company_id
    -- (qui porte le fournisseur). Filtre : commande faite/validée + livraison EN_ATTENTE, < N jours.
    select
        product_destination_id as company_id,
        product_id,
        sum(quantity) as encours_fournisseur
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__commande_fournisseur_tasks`
    where
        task_status_code in ('FAIT', 'VALIDE')
        and delivery_status_code = 'EN_ATTENTE'
        and date(task_start_date) >= date_sub(current_date(), interval 60 day)
    group by product_destination_id, product_id

),

assemble as (

    select
        p.company_id,
        p.product_id,
        p.company_code,
        p.company_name,
        p.product_code,
        p.product_name,
        p.statut_vie,
        p.classe_abc,
        p.methode_prevision,
        p.demande_prevue_mensuelle,
        p.demande_prevue_journaliere,
        coalesce(v.sigma_demande_mensuelle, 0) as sigma_demande_mensuelle,
        coalesce(ep.nb_mois_erreur, 0) as nb_mois_erreur,
        ep.sigma_erreur_mensuelle,
        ep.biais_mensuel,
        coalesce(da.delai_jours_article, dg.delai_jours_global) as delai_jours,
        coalesce(s.stock_actuel, 0) as stock_actuel,
        coalesce(e.encours_fournisseur, 0) as encours_fournisseur,
        -- Niveau de service -> z, piloté par la classe ABC (défaut C pour un couple non classé).
        case p.classe_abc
            when 'A' then 2.05
            when 'B' then 1.645
            else 1.28
        end as z_service,
        -- Conditionnement de commande (unité d'achat, cf. dim) : coeff = nb d'unités par carton/pack.
        prod.purchase_unit_coeff,
        prod.purchase_unit_code
    from prevision as p
    left join variabilite as v on p.company_id = v.company_id and p.product_id = v.product_id
    left join erreur_prevision as ep on p.company_id = ep.company_id and p.product_id = ep.product_id
    left join delai_article as da on p.product_id = da.product_id
    cross join delai_global as dg
    left join stock_actuel as s on p.company_id = s.company_id and p.product_code = s.product_code
    left join encours as e on p.company_id = e.company_id and p.product_id = e.product_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as prod on p.product_id = prod.product_id

),

calcul as (

    select
        *,
        stock_actuel + encours_fournisseur as position_stock,
        -- Horizon à couvrir = délai fournisseur + période de revue (intervalle entre 2 passations,
        -- var revue_jours, défaut 0 = commande possible tous les jours — cf. question 6 mail Vincent).
        delai_jours + 0 as horizon_jours,
        -- σ fiable si assez de mois d'erreur observés au backtest, sinon fallback σ(demande).
        coalesce(
            nb_mois_erreur >= 6
            and sigma_erreur_mensuelle is not null,
            false
        ) as is_sigma_erreur_fiable
    from assemble

),

securite as (

    select
        *,
        case when is_sigma_erreur_fiable then 'erreur' else 'demande_fallback' end as source_sigma,
        -- σ retenu ramené à l'horizon : σ(erreur) si fiable, sinon σ(demande).
        case
            when is_sigma_erreur_fiable then sigma_erreur_mensuelle
            else sigma_demande_mensuelle
        end * sqrt(horizon_jours / 30.4) as sigma_lead_time,
        -- Demande retenue = prévision ③ + correction de biais positif (sous-prévision) si σ fiable.
        -- Le biais négatif (sur-prévision) n'est PAS retranché (prudence, cf. note design V2.1).
        (
            demande_prevue_mensuelle
            + case when is_sigma_erreur_fiable then greatest(biais_mensuel, 0) else 0 end
        ) / 30.4 as demande_retenue_journaliere
    from calcul

),

final as (

    select
        *,
        round(z_service * sigma_lead_time, 2) as stock_securite,
        round(
            demande_retenue_journaliere * horizon_jours + z_service * sigma_lead_time, 2
        ) as point_commande
    from securite

),

suggestion as (

    select
        *,
        -- Feu tricolore d'action. exclu = article arrêté/inactif (③ ne prévoit rien).
        case
            when methode_prevision = 'exclu' then 'exclu'
            when position_stock <= stock_securite then 'rupture'
            when position_stock <= point_commande then 'a_commander'
            else 'ok'
        end as statut_reappro,
        -- Quantité suggérée en unités : combler l'écart jusqu'au point de commande (0 si couvert/exclu).
        case
            when methode_prevision = 'exclu' then 0
            else greatest(0, round(point_commande - position_stock, 2))
        end as quantite_a_commander
    from final

)

select
    current_date() as date_calcul,
    company_id,
    product_id,
    company_code,
    company_name,
    product_code,
    product_name,
    statut_vie,
    classe_abc,
    methode_prevision,
    purchase_unit_code as unite_commande_code,
    round(delai_jours, 1) as delai_jours,
    z_service,
    round(demande_prevue_mensuelle, 2) as demande_prevue_mensuelle,
    round(demande_prevue_journaliere, 3) as demande_prevue_journaliere,
    round(demande_retenue_journaliere, 3) as demande_retenue_journaliere,
    -- Bloc monitoring V2 (fondation du calage de la sécurité — cf. backtest erreur_prevision).
    source_sigma,
    nb_mois_erreur,
    round(sigma_erreur_mensuelle, 2) as sigma_erreur_mensuelle,
    round(sigma_demande_mensuelle, 2) as sigma_demande_mensuelle,
    round(biais_mensuel, 2) as biais_mensuel,
    round(stock_actuel, 2) as stock_actuel,
    round(encours_fournisseur, 2) as encours_fournisseur,
    round(position_stock, 2) as position_stock,
    stock_securite,
    point_commande,
    statut_reappro,
    coalesce(purchase_unit_coeff, 1) as coeff_conditionnement,
    quantite_a_commander,
    -- Reconditionnement : quantité arrondie au conditionnement de commande supérieur (carton/pack).
    -- fallback coeff 1 = commande à l'unité (article sans unité d'achat au master, ~8 produits actifs).
    case
        when methode_prevision = 'exclu' then 0
        else cast(ceil(quantite_a_commander / coalesce(purchase_unit_coeff, 1)) as int64)
    end as quantite_a_commander_conditionnee
from suggestion