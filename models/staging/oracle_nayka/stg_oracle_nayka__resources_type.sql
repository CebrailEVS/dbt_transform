{{
    config(
        materialized='table',
        description='Types de ressources Nayka (Oracle resources_type) : personne, véhicule, sous-traitant…'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_resources_type') }}
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idresources_type as int64) as idresources_type,
        code,
        code_type,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
