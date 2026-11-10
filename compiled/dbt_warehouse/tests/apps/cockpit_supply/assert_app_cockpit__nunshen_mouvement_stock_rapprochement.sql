-- Rapprochement en quantité, par dépôt et article : photo de stock précédente + mouvements passés entre
-- les deux photos (mouvement_at) = photo suivante. Un type de mouvement absent du modèle crée des centaines d'écarts ;
-- quelques écarts isolés viennent de lignes modifiées après leur saisie (Sage ne garde que l'état
-- courant). Porte sur les deux derniers intervalles entre photos gardées (fin de mois et photo courante).

with photos as (
    select distinct
        stock_date,
        extracted_at
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_stock_photo`
),

intervalles as (
    select
        stock_date,
        extracted_at,
        lag(stock_date) over (order by extracted_at) as stock_date_prec,
        lag(extracted_at) over (order by extracted_at) as extracted_at_prec
    from photos
    qualify row_number() over (order by extracted_at desc) <= 2
),

stock as (
    select
        stock_date,
        depot_id,
        reference,
        sum(stock_reel) as quantite
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_stock_photo`
    group by stock_date, depot_id, reference
),

mouvements as (
    select
        i.stock_date,
        m.depot_id,
        m.reference,
        sum(m.quantite) as quantite
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_mouvement_stock` as m
    inner join intervalles as i
        on m.mouvement_at > i.extracted_at_prec and m.mouvement_at <= i.extracted_at
    where m.sens != 'aucun'
    group by i.stock_date, m.depot_id, m.reference
),

cles as (
    select i.stock_date, s.depot_id, s.reference
    from intervalles as i
    inner join stock as s
        on s.stock_date in (i.stock_date, i.stock_date_prec)
    where i.stock_date_prec is not null
    union distinct
    select stock_date, depot_id, reference
    from mouvements
)

select
    c.stock_date,
    i.stock_date_prec,
    c.depot_id,
    c.reference,
    coalesce(o.quantite, 0) as stock_ouverture,
    coalesce(m.quantite, 0) as mouvements,
    coalesce(f.quantite, 0) as stock_cloture,
    coalesce(o.quantite, 0) + coalesce(m.quantite, 0) - coalesce(f.quantite, 0) as ecart
from cles as c
inner join intervalles as i
    on c.stock_date = i.stock_date
left join stock as o
    on i.stock_date_prec = o.stock_date and c.depot_id = o.depot_id and c.reference = o.reference
left join stock as f
    on c.stock_date = f.stock_date and c.depot_id = f.depot_id and c.reference = f.reference
left join mouvements as m
    on c.stock_date = m.stock_date and c.depot_id = m.depot_id and c.reference = m.reference
where
    i.stock_date_prec is not null
    and abs(coalesce(o.quantite, 0) + coalesce(m.quantite, 0) - coalesce(f.quantite, 0)) > 0.001