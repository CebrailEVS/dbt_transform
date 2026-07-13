
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`
      
    
    cluster by company_id, product_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Suggestion de r\u00e9approvisionnement par d\u00e9p\u00f4t et article Neshu \u2014 le maillon final du forecast : \u00e0 partir de la pr\u00e9vision de demande, du stock actuel, du d\u00e9lai fournisseur, des commandes en cours et d'un stock de s\u00e9curit\u00e9 calibr\u00e9 par classe ABC, calcule un point de commande, un feu tricolore (rupture / a_commander / ok / exclu) et une quantit\u00e9 \u00e0 commander. Construite EN PARALL\u00c8LE de fct_supply_chain__couverture_stock_neshu (m\u00e9thode point de commande vs couverture en jours), pour comparaison avant bascule Power BI.\n[COMMENT CONSTRUITE] Base = fct_supply_chain__prevision_demande_neshu (demande_prevue_journaliere/mensuelle, classe_abc, methode_prevision). sigma_demande_mensuelle = \u00e9cart-type de la demande mensuelle par couple (socle int_oracle_neshu__demande_mensuelle, vie active sparse). delai_jours = d\u00e9lai fournisseur moyen PAR ARTICLE (int_oracle_neshu__reception_tasks, is_lead_time_valid, 12 mois), fallback m\u00e9diane globale si l'article n'a pas de r\u00e9ception r\u00e9cente. stock_actuel = dernier snapshot de fct_supply_chain__stock_neshu (entity_type='company', 5 d\u00e9p\u00f4ts de distribution : 114/116/118/120/364 ; hors p\u00e9rim\u00e9s 251, rebus 244, v\u00e9hicules). encours_fournisseur = somme des commandes fournisseur \u00e9mises non re\u00e7ues (int_oracle_neshu__commande_fournisseur_tasks, task_status_code IN ('FAIT','VALIDE') + delivery_status_code='EN_ATTENTE', < 60 j), rattach\u00e9es au d\u00e9p\u00f4t par product_destination_id. Formules : position_stock = stock_actuel + encours ; horizon_jours = delai_jours + revue_jours (p\u00e9riode de revue = intervalle entre 2 passations de commande, var, d\u00e9faut 0 = commande possible tous les jours) ; sigma_lead_time = sigma_demande_mensuelle \u00d7 \u221a(horizon_jours / 30,4) ; stock_securite = z(service ABC) \u00d7 sigma_lead_time ; point_commande = demande_prevue_journaliere \u00d7 horizon_jours + stock_securite ; quantite_a_commander = max(0, point_commande \u2212 position_stock). Reconditionnement : quantite_a_commander_conditionnee = plafond(quantite_a_commander / coeff_conditionnement), o\u00f9 coeff_conditionnement = purchase_unit_coeff de dim_neshu__product (unit\u00e9 d'achat, idunit_type=1 ; fallback 1 = commande \u00e0 l'unit\u00e9). Vars dbt m\u00e9tier : z_service_a/b/c (2,05/1,645/1,28 = service 98/95/90 %), encours_max_jours (60), revue_jours (0 \u2014 \u00e0 caler sur la cadence r\u00e9elle de commande, question pos\u00e9e au m\u00e9tier).\n[GRAIN] 1 ligne par (company_id, product_id) \u2014 \u00e9tat courant recalcul\u00e9 \u00e0 chaque build, align\u00e9 sur la classification \u2461 et la pr\u00e9vision \u2462.\n[NOTES] V1. Le stock de s\u00e9curit\u00e9 ram\u00e8ne la variabilit\u00e9 mensuelle \u00e0 la dur\u00e9e du d\u00e9lai (\u221a(delai/30,4)) : sans ce facteur il serait surdimensionn\u00e9 (~\u00d73 pour un d\u00e9lai de 4 j). L'ABC pilote UNIQUEMENT le niveau de service (z), jamais l'exclusion (exclu = cycle de vie via \u2462). Chaque d\u00e9p\u00f4t principal (Rungis, Lyon, Marseille) g\u00e8re son propre stock et ses propres commandes fournisseur (rattach\u00e9es par product_destination_id) ; Bordeaux/Strasbourg passent peu ou pas de commandes directes (\u00e0 confirmer m\u00e9tier). LIMITES : (1) un stock th\u00e9orique n\u00e9gatif (ghost stock) d\u00e9clenche rupture et gonfle la quantit\u00e9 sugg\u00e9r\u00e9e \u2014 comportement d\u00e9fendable (\u00e0 corriger \u00e0 la source) ; (2) ~22 % des couples n'ont pas de ligne d'inventaire au dernier snapshot (stock_actuel=0 par d\u00e9faut) ; (3) sigma calcul\u00e9 sur mois observ\u00e9s (sparse) et sur toute la vie active \u2014 non d\u00e9saisonnalis\u00e9, donc sur-estim\u00e9 pour les saisonniers et sous-estim\u00e9 pour les articles r\u00e9cents (raffinement V2 = sigma m\u00eame-saison ou sigma de l'erreur de pr\u00e9vision backtest\u00e9e) ; (4) reconditionnement bas\u00e9 sur le master evs_product_unit (Option A) : ~8 produits actifs sans unit\u00e9 d'achat au master tombent sur le fallback \u00ab \u00e0 l'unit\u00e9 \u00bb (coeff 1) \u2014 \u00e0 compl\u00e9ter c\u00f4t\u00e9 ERP. date_calcul = date d'ex\u00e9cution (photo du jour, non historis\u00e9e).\n"""
    )
    as (
      

-- Point de commande NESHU par dépôt et article (maillon 4 : la suggestion de réappro).
-- Assemble la prévision (③), le stock actuel, le délai fournisseur, les commandes en cours et un
-- stock de sécurité calibré par classe ABC, puis émet une quantité à commander + un feu tricolore.
-- Construit EN PARALLÈLE de fct_supply_chain__couverture_stock_neshu (bascule PBI après comparaison).
--
-- Formules (point de commande, gestion de stock standard) :
--   position_stock  = stock_actuel + encours_fournisseur
--   delai_jours     = délai fournisseur moyen PAR ARTICLE (réceptions 12 mois), fallback médiane globale
--   horizon_jours   = delai_jours + revue_jours (période entre 2 passations de commande, var, défaut 0 :
--                     en attente de la cadence réelle de commande côté métier — question 6 du mail)
--   sigma_lead_time = σ(demande mensuelle) × √(horizon / 30.4)  -- variabilité ramenée à l'horizon couvert
--   stock_securite  = z(niveau de service ABC) × sigma_lead_time  -- z : A=2,05 (98%) / B=1,645 (95%) / C=1,28 (90%)
--   point_commande  = demande_prevue_journaliere × horizon + stock_securite
--   qte_a_commander = greatest(0, point_commande − position_stock)
--   statut          = rupture (position ≤ sécurité) / a_commander (≤ point de commande) / ok / exclu
--
-- Grain : 1 ligne par (dépôt, article), aligné sur la classification ② et la prévision ③.
-- Périmètre encours : commande fournisseur FAIT/VALIDE + livraison EN_ATTENTE, < 60 j (règle métier, var).
-- NOTE : chaque dépôt principal (Rungis, Lyon, Marseille) gère son propre stock et passe ses propres
-- commandes fournisseur, rattachées par product_destination_id. Bordeaux/Strasbourg : peu ou pas de
-- commandes directes (à confirmer avec le métier).
-- Paramètres métier en var() dbt : z_service_a/b/c, encours_max_jours (défauts = proposition Vincent).

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
    -- null si < 2 mois de demande -> ramené à 0 (pas de sécurité calculable). Limite V1 : les mois
    -- à zéro ne sont pas densifiés, donc σ légèrement sous-estimé (raffinement V2 : σ de l'erreur).
    select
        company_id,
        product_id,
        coalesce(stddev_samp(quantite_demandee), 0) as sigma_demande_mensuelle
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle`
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
        -- σ ramené à l'horizon (variabilité indépendante jour à jour) : σ_mois × √(horizon/30.4).
        sigma_demande_mensuelle
        * sqrt((delai_jours + 0) / 30.4) as sigma_lead_time
    from assemble

),

final as (

    select
        *,
        round(z_service * sigma_lead_time, 2) as stock_securite,
        round(demande_prevue_journaliere * horizon_jours + z_service * sigma_lead_time, 2) as point_commande
    from calcul

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
    round(sigma_demande_mensuelle, 2) as sigma_demande_mensuelle,
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
    );
  