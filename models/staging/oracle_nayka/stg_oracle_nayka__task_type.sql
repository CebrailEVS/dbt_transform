{{
    config(
        materialized='table',
        description="Types de tâches Nayka (Oracle task_type). code est le libellé propre à l'instance, non unique ; code_type est le type fonctionnel standard de l'ERP (CUSTDELIVERY, SUPPRECEPTION…)."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_task_type') }}
),

cleaned_data as (
    -- Retirées : idstring et idfinancial_group (tables non extraites), iddesktop, mobility_window,
    -- iscreate_task et les réglages EasyMobile / NF525 (configuration de l'interface ERP).
    select
        cast(idtask_type as int64) as idtask_type,
        code,
        code_type,
        cast(coeff as int64) as coeff,
        cast(worked as int64) as worked,
        cast(cast(has_product as int64) as bool) as has_product,
        cast(cast(has_device as int64) as bool) as has_device,
        cast(use_exercice as int64) as use_exercice,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
