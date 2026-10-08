

-- Ruptures des dépôts TechCare : dernière photo seulement (statut du jour des tuiles
-- Dépôt, ISO, Vue d'ensemble). L'app y applique ses règles (fournisseur, code article).
with derniere as (
    select max(stock_date) as stock_date
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
)
select
    r.stock_date,
    r.depot,
    r.reference,
    r.designation,
    r.rupture_statut,
    r.derniere_conso_date,
    r.is_out_of_stock_depot,
    r.is_out_of_stock_global,
    r.qty_depot,
    r.qty_vans_depot,
    r.qty_vans_total,
    r.qty_autres_depots,
    r.nb_conso_180j
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman` as r
inner join derniere as d
    on r.stock_date = d.stock_date