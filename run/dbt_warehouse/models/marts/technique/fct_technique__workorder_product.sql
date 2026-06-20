
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__workorder_product`
      
    partition by timestamp_trunc(date_done, day)
    cluster by product_id, client_id, site_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nConsommation d'articles (pi\u00e8ces d\u00e9tach\u00e9es, consommables) par\nintervention Yuman. Permet l'analyse des pi\u00e8ces consomm\u00e9es par client /\nsite / machine / technicien, et le rapprochement avec les stocks\nth\u00e9oriques (`fct_supply_chain__stock_yuman`).\n\n[COMMENT CONSTRUITE]\nAgr\u00e9gation de `stg_yuman__workorder_products` (lignes de saisie) au\ngrain workorder x produit (`sum(quantity)`), enrichie via\n`int_yuman__demands_workorders_enriched` (d\u00e9dupliqu\u00e9 par workorder)\npour porter les FK client / site / mat\u00e9riel / technicien et la date\nd'intervention.\n\n[GRAIN]\n1 ligne par couple (`workorder_id`, `product_id`).\n\n[NOTES]\nLes workorders hors flux demande (~6) ont leurs FK et `date_done` null\n(left join sur l'intermediate). Pas de valorisation dans le fait : les\nprix Yuman sont courants, sans historique \u2014 valoriser une consommation\npass\u00e9e au prix du jour serait faux (calcul indicatif possible c\u00f4t\u00e9 PBI\nvia `dim_technique__product`).\n"""
    )
    as (
      

with consumed_products as (
    -- Grain source = ligne de saisie (workorder_product_id) : un même produit
    -- peut être saisi plusieurs fois sur un workorder. On agrège au grain
    -- analytique workorder x produit.
    select
        workorder_id,
        product_id,
        sum(product_quantity) as quantity,
        min(product_created_at) as first_recorded_at,
        max(product_updated_at) as last_updated_at
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_products`
    group by workorder_id, product_id
),

workorders as (
    select
        workorder_id,
        demand_id,
        material_id,
        site_id,
        client_id,
        technician_id,
        partner_name,
        workorder_number,
        workorder_type,
        workorder_category,
        workorder_status,
        demand_category_name,
        demand_status,
        is_workorder_paused,
        is_workorder_not_done,
        date_done
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__demands_workorders_enriched`
    where workorder_id is not null
    qualify row_number() over (partition by workorder_id order by date_done desc) = 1
)

select
    cp.workorder_id,
    w.demand_id,
    cp.product_id,
    w.material_id,
    w.site_id,
    w.client_id,
    w.technician_id,
    w.partner_name,
    w.workorder_number,
    w.workorder_type,
    w.workorder_category,
    w.workorder_status,
    w.demand_category_name,
    w.demand_status,
    w.is_workorder_paused,
    w.is_workorder_not_done,
    w.date_done,
    cp.quantity,
    cp.first_recorded_at,
    cp.last_updated_at
from consumed_products as cp
left join workorders as w
    on cp.workorder_id = w.workorder_id
    );
  