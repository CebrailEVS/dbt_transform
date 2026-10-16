{{
    config(
        materialized='table',
        description="Conditions de paiement Nayka (Oracle payment_condition) : délai d'échéance des factures (FACT_30J, FACT_45J…)."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_payment_condition') }}
),

cleaned_data as (
    -- Retirées : idstring (table string non extraite), idinstance (constante), tmp_update et
    -- comments (vides), early_calculation, unit_due et accruing (paramètres internes du calcul d'échéance).
    select
        cast(idpayment_condition as int64) as idpayment_condition,
        code,
        cast(installment_days_number as int64) as installment_days_number,
        cast(installment_month_day_number as int64) as installment_month_day_number,
        cast(multiple_installment as int64) as multiple_installment,
        cast(is_active as bool) as is_active,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
