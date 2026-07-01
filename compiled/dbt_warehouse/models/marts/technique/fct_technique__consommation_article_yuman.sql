

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