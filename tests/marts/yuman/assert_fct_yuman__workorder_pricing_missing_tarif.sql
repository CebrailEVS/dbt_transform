{{
    config(severity='warn')
}}

-- Avertit si des interventions facturables n'ont pas de tarif configuré (amount is null).
-- Cela peut arriver pour de nouvelles clés machine/partenaire pas encore ajoutées au CSV.
-- N'est pas bloquant — severity: warn.
-- 0 lignes retournées = test OK.

select
    workorder_id,
    demand_id,
    date(date_done) as date_done,
    partner_name,
    pricing_key_used,
    pricing_type,
    billing_validation_status
from {{ ref('fct_yuman__workorder_pricing') }}
where to_invoice = true
    and date_done is not null
    and amount is null
