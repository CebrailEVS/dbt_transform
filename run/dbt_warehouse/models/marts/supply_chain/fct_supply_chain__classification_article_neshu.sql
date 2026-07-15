
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
      
    
    cluster by company_id, product_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Classification des articles Neshu par d\u00e9p\u00f4t pour piloter le forecast/r\u00e9appro : l'\"aiguilleur\" qui range chaque couple (d\u00e9p\u00f4t, article) selon son comportement de demande, son cycle de vie et son importance, et en d\u00e9duit la m\u00e9thode de pr\u00e9vision \u00e0 appliquer. N'\u00e9met AUCUN forecast (calcul\u00e9 par le mod\u00e8le de pr\u00e9vision en aval).\n[COMMENT CONSTRUITE] \u00c0 partir du socle int_oracle_neshu__demande_mensuelle. Stats de demande sur la vie active (min\u2192max des mois observ\u00e9s, sparse) : adi = span / nb de mois avec demande, cv2 = (\u00e9cart-type/moyenne)\u00b2 des mois non nuls. classe_demande via la grille Syntetos-Boylan (seuils ADI 1,32 / CV\u00b2 0,49 ; indetermine si < 3 mois d'historique). statut_vie (ACTION) au grain couple : actif si vente < 4 mois (le r\u00e9el gagne, quel que soit le label), hors_saison (saisonnier silencieux hors de sa saison), sinon arrete (product_exploit=NON) ou inactif. Saisonnalit\u00e9 = plano \u00e9t\u00e9/hiver (dim_neshu__product) \u00d7 calendrier ISO (seed ref_general__calendrier_saison) : est_saisonnier (une seule saison plano \u00e0 OUI), en_saison (dans sa saison ce jour). alerte_exploit (WARNING, grain ARTICLE) : arrete_mais_vend / actif_mais_silencieux (label exploit \u2194 r\u00e9el). alerte_saison (WARNING, grain ARTICLE) : plano hiver qui vend \u2265 25 % l'\u00e9t\u00e9, ou plano \u00e9t\u00e9 qui vend < 50 % l'\u00e9t\u00e9 (Oasis/Orangina). classe_abc = Pareto en VALEUR (\u20ac = demande_12m \u00d7 prix d'achat), cumul\u00e9 par d\u00e9p\u00f4t (A \u2264 80 % / B \u2264 95 % / C). methode_prevision = exclu (arrete/inactif), reference_saisonniere (saisonnier avec \u2265 13 mois d'historique), naif (indetermine), moyenne_mobile_surveille (intermittent/lumpy), moyenne_mobile sinon. P\u00e9rim\u00e8tre : 3 d\u00e9p\u00f4ts qui commandent au fournisseur (Rungis/Lyon/Marseille). Strasbourg est r\u00e9approvisionn\u00e9 DEPUIS Lyon \u2192 sa demande est rattach\u00e9e \u00e0 Lyon en amont (socle int_oracle_neshu__demande_mensuelle, macro neshu_depot_reappro), il ne figure donc plus comme d\u00e9p\u00f4t distinct ; Bordeaux commande ind\u00e9pendamment (Distrilog), exclu. Articles product_owner='Neshu', hors familles PRESENTOIR/PRESTATION SERVICE et hors BADGENESHU. company_code/company_name aplatis depuis dim_neshu__company (pattern hybride, propag\u00e9s \u00e0 la pr\u00e9vision et au point de commande).\n[GRAIN] 1 ligne par (company_id, product_id) \u2014 \u00e9tat courant recalcul\u00e9 \u00e0 chaque build.\n[NOTES] V1. L'ABC ne pilote QUE le niveau de service (stock de s\u00e9curit\u00e9 en aval), JAMAIS l'exclusion (exclu = cycle de vie uniquement : un article classe C mais actif est pr\u00e9vu). alerte_exploit au grain article (le label exploit est article-level) pour \u00e9viter les faux positifs d'un article actif ailleurs mais silencieux \u00e0 un d\u00e9p\u00f4t ; elle s'auto-r\u00e9sout quand le m\u00e9tier corrige le label. Saisonnalit\u00e9 : les saisonniers hors-saison passent en statut hors_saison (pas exclus) et, avec \u2265 13 mois d'historique, sont rout\u00e9s vers reference_saisonniere (anticipation N-1) dans la pr\u00e9vision ; alerte_saison signale les plano incoh\u00e9rents avec le comportement r\u00e9el. La d\u00e9tection saisonni\u00e8re repose enti\u00e8rement sur les labels plano : un saisonnier NON \u00e9tiquet\u00e9 n'est pas d\u00e9tect\u00e9 (pas de filet automatique, contrairement au cycle de vie). Croston/SBA (intermittent/lumpy) = V2 ; en V1 ils passent en moyenne_mobile_surveille. date_calcul = date d'ex\u00e9cution (photo du jour, non historis\u00e9e).\n"""
    )
    as (
      

-- Classification article × dépôt pour le forecast/réappro NESHU : l'"aiguilleur".
-- Range chaque couple (dépôt, article) selon 3 axes, puis en déduit la méthode de prévision.
-- N'émet AUCUN forecast (ça, c'est le maillon suivant) :
--   - comportement de demande : Syntetos-Boylan (ADI / CV²) sur la vie active ;
--   - cycle de vie : label product_exploit (autorité) + comportement (filet de sécurité) ;
--   - importance : ABC en VALEUR (€), Pareto par dépôt.
-- Périmètre : 3 dépôts qui commandent au fournisseur (Rungis, Lyon, Marseille). Strasbourg est
-- réapprovisionné depuis Lyon → sa demande est rattachée à Lyon en amont (socle) ; Bordeaux est
-- indépendant (Distrilog), exclu. Hors ateliers / périmés / rebus / stock Nunshen et entités
-- non-dépôt, articles Neshu, hors familles non-consommables (PRESENTOIR, PRESTATION SERVICE) et
-- hors BADGENESHU. Représentation : 1 ligne par (dépôt, article) — état courant.
-- Saisonnalité : plano été/hiver (dim produit) × calendrier ISO (seed ref_general__calendrier_saison)
-- → statut 'hors_saison' (silence hors-saison ≠ mort) + route les saisonniers AVEC historique vers
-- la méthode 'reference_saisonniere' (anticipation via N-1, cf. maillon prévision).

with demande as (

    select
        company_id,
        product_id,
        product_code,
        demande_mois,
        quantite_demandee
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle`

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

part_ete as (

    -- Part du volume vendu en mois d'été (juin→sept) par article, sur 24 mois. Sert à détecter
    -- les mislabels de saisonnalité : un plano « hiver » avec une forte part d'été (ou l'inverse)
    -- = étiquette incohérente avec le comportement (alerte_saison).
    select
        product_id,
        safe_divide(
            sum(if(extract(month from demande_mois) in (6, 7, 8, 9), quantite_demandee, 0)),
            sum(quantite_demandee)
        ) as part_volume_ete
    from demande
    where demande_mois >= date_sub(date_trunc(current_date(), month), interval 24 month)
    group by product_id

),

depots as (

    -- Périmètre de pilotage = les 3 dépôts qui commandent au fournisseur : Rungis, Lyon, Marseille.
    -- Strasbourg (118) est réapprovisionné DEPUIS Lyon : sa demande est déjà rattachée à Lyon en
    -- amont (socle int_oracle_neshu__demande_mensuelle, macro neshu_depot_reappro), donc il ne
    -- figure plus comme dépôt distinct. Bordeaux (120) commande de façon indépendante (Distrilog),
    -- exclu du pilotage. Exclut aussi ateliers, périmés, rebus, stock Nunshen et entités non-dépôt.
    -- code/nom aplatis (pattern hybride).
    select
        company_id,
        company_code,
        company_name
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
    where company_code in ('DEPOTRUNGIS', 'DEPOTLYON', 'DEPOTMARSEILLE')

),

produit as (

    -- Périmètre (dépôts + articles) + enrichissement produit (label exploit, prix pour l'ABC valeur).
    select
        s.*,
        d.company_code,
        d.company_name,
        p.product_name,
        p.product_exploit,
        p.product_planoete,
        p.product_planohiver,
        s.demande_12m * coalesce(p.purchase_unit_price, 0) as valeur_12m
    from stats as s
    inner join depots as d on s.company_id = d.company_id
    inner join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p on s.product_id = p.product_id
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

saison_courante as (

    -- Saison de la semaine ISO en cours (été / hiver), depuis le calendrier logistique.
    select saison
    from `evs-datastack-prod`.`prod_reference`.`ref_general__calendrier_saison`
    where numero_semaine = extract(isoweek from current_date())

),

saisonnalite as (

    -- Enrichissement saisonnalité (plano × calendrier) :
    --  - est_saisonnier : l'article n'est vendu qu'une saison (une seule des 2 plano à OUI) ;
    --  - saison_produit : ete / hiver / annuel / non_renseigne ;
    --  - en_saison : l'article est-il DANS sa saison à la date du jour ;
    --  - mois_depuis_premiere : ancienneté (≥ 13 → un N-1 saisonnier existe).
    select
        a.*,
        (a.product_planoete = 'OUI' and a.product_planohiver = 'NON')
        or (a.product_planoete = 'NON' and a.product_planohiver = 'OUI') as est_saisonnier,
        case
            when a.product_planoete = 'OUI' and a.product_planohiver = 'NON' then 'ete'
            when a.product_planoete = 'NON' and a.product_planohiver = 'OUI' then 'hiver'
            when a.product_planoete = 'OUI' and a.product_planohiver = 'OUI' then 'annuel'
            else 'non_renseigne'
        end as saison_produit,
        (a.product_planoete = 'OUI' and sc.saison = 'ete')
        or (a.product_planohiver = 'OUI' and sc.saison = 'hiver') as en_saison,
        date_diff(date_trunc(current_date(), month), a.premiere_sortie, month) as mois_depuis_premiere,
        pe.part_volume_ete
    from abc as a
    cross join saison_courante as sc
    left join part_ete as pe on a.product_id = pe.product_id

),

classifie as (

    select
        *,
        -- Statut = ACTION (prévoir / exclure), au grain couple : un article qui vend récemment à
        -- ce dépôt est 'actif' quel que soit son label (le réel gagne, jamais de rupture manquée).
        -- hors_saison = saisonnier silencieux car HORS de sa saison (≠ mort) → à anticiper, pas exclu.
        -- L'incohérence de label est portée à part, au grain article, par alerte_label.
        case
            when mois_inactif < 4 then 'actif'
            when product_exploit = 'NON' then 'arrete'
            when est_saisonnier and not en_saison then 'hors_saison'
            else 'inactif'
        end as statut_vie,
        -- Alerte incohérence du label EXPLOIT ↔ réel (grain ARTICLE) : évite les faux positifs
        -- (actif ailleurs mais silencieux à ce dépôt ; saisonnier silencieux hors saison).
        case
            when product_exploit = 'NON' and mois_inactif_article < 4 then 'arrete_mais_vend'
            when
                product_exploit = 'OUI' and mois_inactif_article >= 4
                and not (est_saisonnier and not en_saison) then 'actif_mais_silencieux'
        end as alerte_exploit,
        -- Alerte incohérence du label SAISON (plano) ↔ comportement réel : un plano « hiver » qui
        -- vend ≥ 25 % l'été (ou « été » qui vend > 50 % hors été) = étiquette à corriger (ex. Oasis,
        -- Orangina labellisés hiver mais qui vendent toute l'année).
        case
            when est_saisonnier and saison_produit = 'hiver' and part_volume_ete >= 0.25
                then 'saison_hiver_mais_vend_ete'
            when est_saisonnier and saison_produit = 'ete' and part_volume_ete < 0.50
                then 'saison_ete_mais_vend_hiver'
        end as alerte_saison,
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
    from saisonnalite

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
    classe_demande,
    classe_abc,
    -- Méthode V1 : reference_saisonniere pour les saisonniers avec historique (anticipation via
    -- N-1) ; moyenne mobile pour l'essentiel ; Croston (intermittent/lumpy) = V2.
    case
        when statut_vie in ('arrete', 'inactif') then 'exclu'
        when est_saisonnier and mois_depuis_premiere >= 13 then 'reference_saisonniere'
        when classe_demande = 'indetermine' then 'naif'
        when classe_demande in ('intermittent', 'lumpy') then 'moyenne_mobile_surveille'
        else 'moyenne_mobile'
    end as methode_prevision,
    alerte_exploit,
    alerte_saison,
    saison_produit,
    en_saison,
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
    );
  