
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__couverture_stock_neshu`
      
    partition by date_calcul
    cluster by company_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Couverture de stock et suggestion de r\u00e9approvisionnement par d\u00e9p\u00f4t et article Neshu : nombre de jours de stock restants au rythme de consommation de l'an dernier \u00e0 la m\u00eame saison, statut feu tricolore (RUPTURE TOTALE / RUPTURE / VIGILANCE / OK / NON CONSOMME), et quantit\u00e9 sugg\u00e9r\u00e9e \u00e0 commander.\n[COMMENT CONSTRUITE] Stock actuel = dernier snapshot de fct_supply_chain__stock_neshu (d\u00e9p\u00f4ts entity_type='company', hors DEPOTREBUS / DEPOTPERIMES). Consommation = somme des sorties clients (int_oracle_neshu__livraison_tasks) et des chargements v\u00e9hicules (int_oracle_neshu__livraison_interne_tasks, product_source_type='COMPANY' ET product_destination_type!='COMPANY' pour neutraliser les transferts inter-d\u00e9p\u00f4ts), statuts FAIT+VALIDE, sur une fen\u00eatre centr\u00e9e mois N-1 \u00b1 1 (3 mois, pour lisser les faibles rotations), en quantit\u00e9 d\u00e9conditionn\u00e9e \u00e0 l'unit\u00e9 (quantity = real_quantity * unit_coeff_multi / unit_coeff_div ; base_unit_quantity = nb de colis, \u00e0 ne PAS utiliser). conso_journaliere_n1 = conso de la fen\u00eatre / nb de jours r\u00e9el de la fen\u00eatre. jours_couverture = stock_actuel / conso_journaliere_n1. FULL OUTER stock\u22c8conso : un article consomm\u00e9 sur la fen\u00eatre mais absent du stock du jour remonte en RUPTURE TOTALE (stock_actuel=0). product_id/product_name aplatis depuis dim_neshu__product. Produits explicitement arr\u00eat\u00e9s exclus (product_exploit='NON') ; les NULL (champ non renseign\u00e9) sont conserv\u00e9s pour ne pas masquer de ruptures r\u00e9elles \u2014 \u00e0 basculer en product_exploit='OUI' quand le champ sera compl\u00e9t\u00e9 \u00e0 la source. Cible de r\u00e9appro = d\u00e9lai fournisseur (7j) + couverture (21j) = 28j. Seuils/cible param\u00e9trables via vars dbt couverture_seuil_min_jours / couverture_seuil_max_jours / couverture_cible_jours (d\u00e9fauts 14 / 28 / 28).\n[GRAIN] 1 ligne par (date_calcul, company_id, product_code).\n[NOTES] V2. Ne tient PAS encore compte des commandes fournisseurs en cours (pas de source dispo aujourd'hui \u2014 logique Distrilog not\u00e9e pour plus tard : type=Bon de commande fournisseur, statut FAIT/VALIDE, statut livraison=En attente). statut='RUPTURE TOTALE' si demande sur la fen\u00eatre mais stock_actuel<=0 (absent du snapshot ou stock th\u00e9orique n\u00e9gatif), affich\u00e9 en t\u00eate. statut='NON CONSOMME' si aucune conso sur la fen\u00eatre \u2192 jours_couverture NULL et qte_a_commander forc\u00e9e \u00e0 0 (m\u00eame si stock n\u00e9gatif). date_calcul = date d'ex\u00e9cution (photo du jour, non historis\u00e9e).\n"""
    )
    as (
      

with depots as (
    select distinct
        id_entity,
        entity_code,
        entity_name
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
    -- exclusion centralisée ici (par code, plus parlant que les id 244/251) :
    -- DEPOTREBUS = rebus, DEPOTPERIMES = périmés. Propagée en aval via les jointures.
    where
        entity_type = 'company'
        and entity_code not in ('DEPOTREBUS', 'DEPOTPERIMES')
),

dernier_snapshot as (
    select max(date(date_system)) as snapshot_date
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
),

stock_actuel as (
    select
        s.id_entity,
        s.product_code,
        s.stock_at_date as stock_actuel
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu` as s
    cross join dernier_snapshot as d
    where
        date(s.date_system) = d.snapshot_date
        and s.entity_type = 'company'
),

-- fenêtre de consommation centrée sur le même mois l'an dernier (mois N-1 ± 1),
-- pour lisser les articles à faible rotation tout en gardant la saisonnalité
fenetre_reference as (
    select
        date_sub(date_trunc(date_sub(current_date(), interval 1 year), month), interval 1 month) as fenetre_debut,
        date_add(date_trunc(date_sub(current_date(), interval 1 year), month), interval 2 month) as fenetre_fin
),

fenetre_reference_jours as (
    select
        fenetre_debut,
        fenetre_fin,
        date_diff(fenetre_fin, fenetre_debut, day) as nb_jours_fenetre
    from fenetre_reference
),

sorties_clients as (
    select
        t.product_source_id as id_entity,
        t.product_code,
        sum(t.quantity) as quantite_sortie
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_tasks` as t
    cross join fenetre_reference as f
    where
        t.task_status_code in ('FAIT', 'VALIDE')
        and date(t.task_start_date) >= f.fenetre_debut
        and date(t.task_start_date) < f.fenetre_fin
    group by 1, 2
),

sorties_vehicules as (
    select
        t.product_source_id as id_entity,
        t.product_code,
        sum(t.quantity) as quantite_sortie
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_interne_tasks` as t
    cross join fenetre_reference as f
    where
        t.task_status_code in ('FAIT', 'VALIDE')
        and t.product_source_type = 'COMPANY'
        -- neutralisation des transferts inter-dépôts (relocalisation, pas une demande) :
        -- on ne garde que les sorties vers un véhicule (destination != COMPANY)
        and t.product_destination_type != 'COMPANY'
        and date(t.task_start_date) >= f.fenetre_debut
        and date(t.task_start_date) < f.fenetre_fin
    group by 1, 2
),

consommation_n1 as (
    select
        o.id_entity,
        o.product_code,
        sum(o.quantite_sortie) as conso_fenetre,
        safe_divide(sum(o.quantite_sortie), any_value(f.nb_jours_fenetre)) as conso_journaliere_n1
    from (
        select * from sorties_clients
        union all
        select * from sorties_vehicules
    ) as o
    cross join fenetre_reference_jours as f
    group by 1, 2
),

-- full outer : on garde le stock du jour ET les articles consommés sur la fenêtre
-- mais absents du stock actuel (= ruptures totales potentielles, à faire remonter)
base as (
    select
        coalesce(s.id_entity, c.id_entity) as id_entity,
        coalesce(s.product_code, c.product_code) as product_code,
        coalesce(s.stock_actuel, 0) as stock_actuel,
        coalesce(c.conso_journaliere_n1, 0) as conso_journaliere_n1
    from stock_actuel as s
    full outer join consommation_n1 as c
        on
            s.id_entity = c.id_entity
            and s.product_code = c.product_code
),

couverture as (
    select
        d.id_entity as company_id,
        d.entity_code,
        d.entity_name,
        b.product_code,
        p.product_id,
        p.product_name,
        b.stock_actuel,
        b.conso_journaliere_n1,
        safe_divide(b.stock_actuel, b.conso_journaliere_n1) as jours_couverture
    from base as b
    inner join depots as d on b.id_entity = d.id_entity
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p on b.product_code = p.product_code
    -- on exclut uniquement les produits explicitement arrêtés (product_exploit='NON') ;
    -- les NULL (champ non renseigné dans Distrilog) sont conservés pour ne pas masquer
    -- de ruptures réelles. À basculer en '=OUI' quand le champ sera complété à la source.
    where p.product_exploit is distinct from 'NON'
)

select
    current_date() as date_calcul,
    company_id,
    entity_code,
    entity_name,
    product_code,
    product_id,
    product_name,
    stock_actuel,
    round(conso_journaliere_n1, 2) as conso_journaliere_n1,
    round(conso_journaliere_n1 * 30.4, 0) as conso_mensuelle_moy_n1,
    round(jours_couverture, 1) as jours_couverture,
    case
        when conso_journaliere_n1 = 0 then 'NON CONSOMME'
        when stock_actuel <= 0 then 'RUPTURE TOTALE'
        when jours_couverture < 14 then 'RUPTURE'
        when jours_couverture <= 28 then 'VIGILANCE'
        else 'OK'
    end as statut,
    case
        when conso_journaliere_n1 = 0 then 0
        else greatest(
            0,
            round(conso_journaliere_n1 * 28) - stock_actuel
        )
    end as qte_a_commander
from couverture
    );
  