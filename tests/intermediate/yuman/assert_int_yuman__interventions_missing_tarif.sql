{{
    config(severity='warn')
}}

-- Avertit si des interventions facturables n'ont pas de tarif configuré (amount is null).
-- Cela peut arriver pour de nouvelles clés machine/partenaire pas encore ajoutées au CSV.
-- Les interventions hors flux demande (statut UNTRACKABLE, workorder_id null) sont
-- exclues : intraçables par construction, aucun ajout au CSV ne peut les résoudre.
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
from {{ ref('int_yuman__interventions') }}
where
    billing_validation_status = 'MISSING_TARIF'
    and date_done is not null
