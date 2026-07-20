

-- La rupture par dépôt n'existe pas en source : quand un article tombe à zéro,
-- Yuman supprime les lignes d'emplacement (une ligne n'existe que si quantite > 0).
-- On la reconstruit : les 4 dépôts étant fixes et présents chaque jour dans
-- l'export, l'absence de ligne (dépôt, référence) = stock à zéro. L'assortiment
-- attendu d'un dépôt (les références qu'il est censé stocker) est défini par la
-- demande : consommations des 180 jours précédents des techniciens rattachés.

with stock_dates as (
    select distinct date(export_date) as stock_date
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
),

pseudo_articles as (
    select reference from `evs-datastack-prod`.`prod_reference`.`ref_yuman__pseudo_article`
),

conso as (
    -- Consommations rattachées à un dépôt via le technicien, deux flux unis :
    -- les bons Yuman et les interventions Nespresso (Nomad Repair, articles
    -- centralisés dans le référentiel Yuman sous EVS_NESPRESSO_). Flux disjoints
    -- (vérifié métier : outils non mélangés). Dim Type 1 : le rattachement est
    -- l'état courant, appliqué à tout l'historique.
    -- Yuman : seules les interventions réalisées ou en cours comptent (une conso
    -- saisie sur un workorder NON_REALISEE / EN_PAUSE n'est pas une sortie sûre).
    select
        t.entrepot_rattachement as depot,
        c.product_reference as reference,
        date(c.date_done) as conso_date
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_yuman` as c
    inner join `evs-datastack-prod`.`prod_marts`.`dim_technique__technician` as t
        on c.technician_id = t.user_id
    left join pseudo_articles as pa
        on c.product_reference = pa.reference
    where
        c.intervention_state in ('REALISEE', 'EN_COURS')
        and t.entrepot_rattachement is not null
        and c.product_reference is not null
        and pa.reference is null

    union all

    -- Nespresso : aucun filtre d'état (une ligne article = une pose réelle,
    -- décision métier) ; agences EVS déjà filtrées dans le fact amont ;
    -- date de conso = fin d'intervention (décision métier).
    select
        t.entrepot_rattachement as depot,
        n.product_reference as reference,
        date(n.date_heure_fin) as conso_date
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso` as n
    inner join `evs-datastack-prod`.`prod_marts`.`dim_technique__technician` as t
        on n.technician_id = t.user_id
    left join pseudo_articles as pa
        on n.product_reference = pa.reference
    where
        t.entrepot_rattachement is not null
        and pa.reference is null
),

assortiment as (
    -- Assortiment attendu « as-of » chaque date d'export : références consommées
    -- au moins 2 fois sur les 180 jours glissants précédents (seuil validé métier).
    select
        d.stock_date,
        c.depot,
        c.reference,
        count(*) as nb_conso_180j,
        max(c.conso_date) as derniere_conso_date
    from stock_dates as d
    inner join conso as c
        on
            date_sub(d.stock_date, interval 180 day) < c.conso_date
            and d.stock_date >= c.conso_date
    group by d.stock_date, c.depot, c.reference
    having count(*) >= 2
),

vans_technicien as (
    -- Van (magasin Yuman du technicien) -> dépôt de rattachement, pour ventiler
    -- l'autonomie terrain par dépôt.
    select
        storehouses_name,
        entrepot_rattachement as depot
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
    where storehouses_name is not null and entrepot_rattachement is not null
),

stock_ventile as (
    -- Stock du jour par emplacement, qualifié dépôt / van (+ dépôt du van si connu).
    -- Les lignes de rupture source (sans emplacement, quantite = 0) sont ignorées :
    -- l'absence dans ce CTE vaut zéro dans l'agrégation finale.
    select
        date(s.export_date) as stock_date,
        s.reference,
        s.nom_du_stock,
        s.quantite,
        (s.nom_du_stock like '%DEPOT%') as is_depot,
        v.depot as van_depot
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique` as s
    left join vans_technicien as v
        on s.nom_du_stock = v.storehouses_name
    where s.reference is not null and s.nom_du_stock is not null
),

reference_designation as (
    -- Désignation unique par référence : on écarte les libellés « marqueur »
    -- (NE PAS UTILISER, OLD, Remplacé par...), fallback min() sinon.
    select
        reference,
        coalesce(
            min(case when not regexp_contains(upper(designation), r'NE PAS UTILISER|^OLD |REMPLAC.{0,4}PAR') then designation end),
            min(designation)
        ) as designation
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where reference is not null
    group by reference
),

assortiment_stock as (
    select
        a.stock_date,
        a.depot,
        a.reference,
        a.nb_conso_180j,
        a.derniere_conso_date,
        coalesce(sum(case when sv.is_depot and sv.nom_du_stock = a.depot then sv.quantite end), 0) as qty_depot,
        coalesce(sum(case when sv.is_depot and sv.nom_du_stock != a.depot then sv.quantite end), 0)
            as qty_autres_depots,
        coalesce(sum(case when not sv.is_depot and sv.van_depot = a.depot then sv.quantite end), 0)
            as qty_vans_depot,
        coalesce(sum(case when not sv.is_depot then sv.quantite end), 0) as qty_vans_total
    from assortiment as a
    left join stock_ventile as sv
        on a.stock_date = sv.stock_date and a.reference = sv.reference
    group by a.stock_date, a.depot, a.reference, a.nb_conso_180j, a.derniere_conso_date
)

select
    -- Grain
    ast.stock_date,
    ast.depot,
    ast.reference,

    -- Attributs
    rd.designation,
    -- Qualification de la rupture (NULL si en stock). Pas de transfert inter-dépôt
    -- en pratique : STOCK_AILLEURS est informatif, le remède reste la commande.
    case
        when ast.qty_depot > 0 then null
        when ast.qty_depot + ast.qty_autres_depots + ast.qty_vans_total = 0 then 'RUPTURE_TOTALE'
        when ast.qty_vans_depot > 0 then 'STOCK_RESTANT_VANS'
        else 'STOCK_AILLEURS'
    end as rupture_statut,

    -- Dates secondaires
    ast.derniere_conso_date,

    -- Flags
    ast.qty_depot = 0 as is_out_of_stock_depot,
    ast.qty_depot + ast.qty_autres_depots + ast.qty_vans_total = 0 as is_out_of_stock_global,

    -- Mesures
    ast.qty_depot,
    ast.qty_vans_depot,
    ast.qty_vans_total,
    ast.qty_autres_depots,
    ast.nb_conso_180j,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    'e772ede9-2479-492a-b41a-a40fd2c36829' as dbt_invocation_id
from assortiment_stock as ast
left join reference_designation as rd
    on ast.reference = rd.reference