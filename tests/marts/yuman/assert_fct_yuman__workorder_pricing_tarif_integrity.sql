{{
    config(severity='error')
}}

-- Vérifie que chaque intervention facturable a le BON montant selon le référentiel tarifaire.
-- Cas couverts : montant appliqué différent du montant attendu, ou clé absente du référentiel.
-- Ne couvre PAS les interventions sans montant (MISSING_TARIF) — voir le test _warn séparé.
-- 0 lignes retournées = test OK.

with fact as (
    select *
    from {{ ref('fct_yuman__workorder_pricing') }}
    where to_invoice = true
        and date_done is not null
        and amount is not null
),

ref_tarif as (
    select
        lower(concat(
            type_inter, '_',
            machine, '_',
            marque, '_',
            type_tarif, '_',
            cast(metropole as string)
        )) as key_tarif,
        montant,
        valid_from,
        coalesce(valid_to, date('9999-12-31')) as valid_to
    from {{ ref('ref_yuman__tarification_clean') }}
)

select
    f.workorder_id,
    f.demand_id,
    date(f.date_done) as date_done,
    f.partner_name,
    f.pricing_key_used,
    f.pricing_type,
    f.amount as amount_used,
    r.montant as expected_amount
from fact as f
left join ref_tarif as r
    on f.pricing_key_used = r.key_tarif
    and date(f.date_done) between r.valid_from and r.valid_to
where
    f.amount != r.montant
    or (f.amount is not null and r.montant is null)
