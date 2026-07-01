
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_yuman`
      
    partition by timestamp_trunc(date_done, day)
    cluster by product_id, client_id, site_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nConsommation d'articles (pi\u00e8ces d\u00e9tach\u00e9es, consommables) lors des\ninterventions Yuman \u2014 \u00ab article \u00bb au sens catalogue Yuman, \u00e0 ne pas\nconfondre avec la consommation de boissons NESHU (`fct_neshu__consommation`).\nPermet l'analyse des articles consomm\u00e9s par client / site / machine /\ntechnicien / type d'intervention, et le rapprochement avec les stocks\n(`fct_supply_chain__stock_yuman`, `fct_supply_chain__stock_article_yuman`)\nvia `dim_technique__product` (product_code = reference).\n\n[COMMENT CONSTRUITE]\nAgr\u00e9gation de `stg_yuman__workorder_products` (lignes de saisie) au\ngrain workorder x article (`sum(quantity)`), jointe (inner join) \u00e0\n`int_yuman__demands_workorders_enriched` (d\u00e9dupliqu\u00e9 par workorder,\nrestreint aux workorders rattach\u00e9s \u00e0 une demande) pour porter les FK\nclient / site / mat\u00e9riel / technicien, l'\u00e9tat m\u00e9tier canonique\n`intervention_state` et les motifs de non-intervention / pause.\n\n[GRAIN]\n1 ligne par couple (`workorder_id`, `product_id`). ~16,6k lignes.\n\n[NOTES]\nP\u00e9rim\u00e8tre : les workorders **hors flux demande** (sans demande rattach\u00e9e,\n~10 lignes, FK non exploitables) sont **exclus**. Aucun filtre de statut en\nrevanche : ~99,8 % de la conso est sur des interventions REALISEE, le reste\n(NON_REALISEE / EN_COURS / EN_PAUSE) est conserv\u00e9 et qualifi\u00e9 par\n`intervention_state` pour laisser le choix au m\u00e9tier \u2014 filtrer\n`intervention_state = 'REALISEE'` pour la conso confirm\u00e9e. Les NON_REALISEE\nsont majoritairement des \u00ab Erreur de saisie \u00bb (cf.\n`workorder_motif_non_intervention`) \u2192 conso probablement erron\u00e9e. Les\ninterventions EN_COURS ont `date_done` null (partition null). Pas de\nvalorisation : prix Yuman courants sans historique (calcul indicatif\npossible c\u00f4t\u00e9 PBI via `dim_technique__product`).\n"""
    )
    as (
      

with consumed_products as (
    -- Grain source = ligne de saisie (workorder_product_id) : un même produit
    -- peut être saisi plusieurs fois sur un workorder. On agrège au grain
    -- analytique workorder x produit.
    select
        workorder_id,
        product_id,
        -- Référence article = 1:1 avec product_id (et = dim product_code / clé stock)
        min(product_reference) as product_reference,
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
        demand_category_name,
        -- État métier canonique (source de vérité de l'intermediate) + détails
        intervention_state,
        workorder_motif_non_intervention,
        workorder_detail_non_intervention,
        workorder_raison_mise_en_pause,
        workorder_explication_mise_en_pause,
        date_done
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__demands_workorders_enriched`
    -- Exclut les workorders hors flux demande (orphelins / absents de l'intermediate) :
    -- FK non exploitables. Toute conso conservée est rattachée à une demande.
    where workorder_id is not null and demand_id is not null
    qualify row_number() over (partition by workorder_id order by date_done desc) = 1
)

select
    -- Grain (dimensions dégénérées + FK produit)
    cp.workorder_id,
    w.demand_id,
    cp.product_id,

    -- FK dimensions
    w.material_id,
    w.site_id,
    w.client_id,
    w.technician_id,

    -- Attributs d'affichage
    cp.product_reference,
    w.partner_name,
    w.workorder_number,
    w.workorder_type,
    w.workorder_category,
    w.demand_category_name,

    -- État métier de l'intervention (canonique) + motifs
    w.intervention_state,
    w.workorder_motif_non_intervention,
    w.workorder_detail_non_intervention,
    w.workorder_raison_mise_en_pause,
    w.workorder_explication_mise_en_pause,

    -- Date d'intervention
    w.date_done,

    -- Mesure
    cp.quantity,

    -- Métadonnées
    cp.first_recorded_at,
    cp.last_updated_at
from consumed_products as cp
inner join workorders as w
    on cp.workorder_id = w.workorder_id
    );
  