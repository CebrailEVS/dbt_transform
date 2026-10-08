
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_rupture_depot_courant`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Ruptures des d\u00e9p\u00f4ts TechCare au dernier jour connu : statut du jour des tuiles D\u00e9p\u00f4t, ISO et Vue d'ensemble de Cockpit Supply.\n[COMMENT CONSTRUITE] Lignes de fct_supply_chain__rupture_depot_yuman au stock_date maximal, colonnes techniques exclues.\n[GRAIN] 1 ligne par (depot, reference) au dernier stock_date.\n[NOTES] Environ 480 lignes au lieu de l'historique complet. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

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
    );
  