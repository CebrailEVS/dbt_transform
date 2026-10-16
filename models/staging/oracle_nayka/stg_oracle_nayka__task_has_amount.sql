{{
    config(
        materialized='table',
        description='Montants des tâches Nayka par taux de TVA (Oracle task_has_amount) : base HT, TVA et part du total, une ligne par tâche × taux × taxe × région fiscale.'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_task_has_amount') }}
),

cleaned_data as (
    -- _parent_modification_date non reprise : curseur technique de dlt, copie de task.modification_date.
    select
        cast(idtask as int64) as idtask,
        cast(idtax as int64) as idtax,
        cast(idtax_region as int64) as idtax_region,
        cast(tax_rate as numeric) as tax_rate,
        tax_code,
        tax_name,
        cast(amount_without_tax as numeric) as amount_without_tax,
        cast(tax_amount as numeric) as tax_amount,
        cast(percentage as numeric) as percentage,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
