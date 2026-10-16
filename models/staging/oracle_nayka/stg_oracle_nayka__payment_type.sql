{{
    config(
        materialized='table',
        description="Modes de paiement Nayka (Oracle payment_type) : espèces, CB, paiement mobile, et modes créés par l'instance."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_payment_type') }}
),

cleaned_data as (
    -- Retirées : idstring et idregulation_type (tables non extraites), idinstance (constante),
    -- code_type et direct_debit_date (vides), task_type (sens non documenté), export_to_em (mobile).
    select
        cast(idpayment_type as int64) as idpayment_type,
        code,
        cast(system as int64) as system,  -- noqa: RF04
        is_active,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
