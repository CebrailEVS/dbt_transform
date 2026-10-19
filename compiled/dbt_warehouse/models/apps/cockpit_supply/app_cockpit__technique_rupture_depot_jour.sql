

-- Ruptures quotidiennes des dépôts TechCare, étape commune aux modèles rupture_depot_* de l'app (non lue
-- directement). Même calcul que fct_supply_chain__rupture_depot_yuman, refait depuis le staging et l'intermediate
-- pour que l'app ne dépende pas des marts.
-- La rupture par dépôt n'existe pas en source : Yuman supprime la ligne d'emplacement d'un article tombé à zéro.
-- Les dépôts étant présents chaque jour dans l'export, l'absence de ligne (dépôt, référence) vaut stock zéro.
-- L'assortiment attendu d'un dépôt est défini par la demande : consommations des 180 jours précédents des
-- techniciens qui y sont rattachés.
with stock_dates as (
    select distinct date(export_date) as stock_date
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique`
),

pseudo_articles as (
    select reference from `evs-datastack-prod`.`prod_reference`.`ref_yuman__pseudo_article`
),

-- Technicien = utilisateur de type technician, ou manager déclaré technicien. Rattachement au dépôt en état
-- courant, appliqué à tout l'historique.
techniciens as (
    select
        u.user_id,
        u.nomad_id,
        u.entrepot_rattachement,
        s.storehouses_name
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users` as u
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses` as s
        on u.user_id = s.storehouses_id
    where
        u.user_type = 'technician'
        or (u.user_type = 'manager' and u.is_manager_as_technician = true)
),

-- Bons Yuman, au grain workorder × produit (un produit peut être saisi plusieurs fois sur un workorder).
conso_yuman as (
    select
        workorder_id,
        min(product_reference) as product_reference
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_products`
    group by workorder_id, product_id
),

workorders as (
    -- Seuls les workorders rattachés à une demande ; une ligne par workorder.
    select
        workorder_id,
        technician_id,
        intervention_state,
        date_done
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__demands_workorders_enriched`
    where workorder_id is not null and demand_id is not null
    qualify row_number() over (partition by workorder_id order by date_done desc) = 1
),

-- Interventions Nespresso (Nomad Repair) des agences EVS : une ligne article = une pose réelle.
conso_nespresso as (
    select
        a.n_tech,
        concat('EVS_NESPRESSO_', upper(a.code_article)) as product_reference,
        i.date_heure_fin
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as a
    inner join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as i
        on a.n_planning = i.n_planning
    where i.agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
),

conso as (
    -- Yuman : seules les interventions réalisées ou en cours sont des sorties sûres.
    select
        t.entrepot_rattachement as depot,
        c.product_reference as reference,
        date(w.date_done) as conso_date
    from conso_yuman as c
    inner join workorders as w
        on c.workorder_id = w.workorder_id
    inner join techniciens as t
        on w.technician_id = t.user_id
    left join pseudo_articles as pa
        on c.product_reference = pa.reference
    where
        w.intervention_state in ('REALISEE', 'EN_COURS')
        and t.entrepot_rattachement is not null
        and c.product_reference is not null
        and pa.reference is null
    union all
    -- Nespresso : aucun filtre d'état ; date de conso = fin d'intervention (décisions métier). Le technicien est
    -- retrouvé par son matricule Nomad.
    select
        t.entrepot_rattachement as depot,
        n.product_reference as reference,
        date(n.date_heure_fin) as conso_date
    from conso_nespresso as n
    inner join techniciens as t
        on t.nomad_id is not null and lower(n.n_tech) = lower(cast(t.nomad_id as string))
    left join pseudo_articles as pa
        on n.product_reference = pa.reference
    where
        t.entrepot_rattachement is not null
        and pa.reference is null
),

assortiment as (
    -- Références consommées au moins 2 fois sur les 180 jours glissants précédant chaque export (seuil métier).
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
    select
        storehouses_name,
        entrepot_rattachement as depot
    from techniciens
    where storehouses_name is not null and entrepot_rattachement is not null
),

stock_ventile as (
    -- Stock du jour par emplacement, qualifié dépôt ou van (avec le dépôt du van quand il est connu).
    select
        date(s.export_date) as stock_date,
        s.reference,
        s.nom_du_stock,
        s.quantite,
        (s.nom_du_stock like '%DEPOT%') as is_depot,
        v.depot as van_depot
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique` as s
    left join vans_technicien as v
        on s.nom_du_stock = v.storehouses_name
    where s.reference is not null and s.nom_du_stock is not null
),

reference_designation as (
    -- Une désignation par référence, en écartant les libellés « marqueur » (NE PAS UTILISER, OLD…).
    select
        reference,
        coalesce(
            min(case when not regexp_contains(upper(designation), r'NE PAS UTILISER|^OLD |REMPLAC.{0,4}PAR') then designation end),
            min(designation)
        ) as designation
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique`
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
    ast.stock_date,
    ast.depot,
    ast.reference,
    rd.designation,
    -- NULL si en stock. STOCK_AILLEURS est informatif : il n'y a pas de transfert inter-dépôt en pratique.
    case
        when ast.qty_depot > 0 then null
        when ast.qty_depot + ast.qty_autres_depots + ast.qty_vans_total = 0 then 'RUPTURE_TOTALE'
        when ast.qty_vans_depot > 0 then 'STOCK_RESTANT_VANS'
        else 'STOCK_AILLEURS'
    end as rupture_statut,
    ast.derniere_conso_date,
    ast.qty_depot = 0 as is_out_of_stock_depot,
    ast.qty_depot + ast.qty_autres_depots + ast.qty_vans_total = 0 as is_out_of_stock_global,
    ast.qty_depot,
    ast.qty_vans_depot,
    ast.qty_vans_total,
    ast.qty_autres_depots,
    ast.nb_conso_180j
from assortiment_stock as ast
left join reference_designation as rd
    on ast.reference = rd.reference