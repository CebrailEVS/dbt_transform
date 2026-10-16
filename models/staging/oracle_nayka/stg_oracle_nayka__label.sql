{{
    config(
        materialized='table',
        description="Étiquettes Nayka (Oracle label) : valeurs d'une famille d'étiquettes. code n'est unique qu'au sein de sa famille."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_label') }}
),

cleaned_data as (
    -- Retirées : idstring (table string non extraite), idinstance (constante),
    -- display_color (jamais renseignée).
    select
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,
        -- trim : code sert de clé aux pivots en aval, et la saisie laisse des espaces.
        trim(code) as code,
        cast(enabled as int64) as enabled,
        cast(isdefault as int64) as isdefault,
        cast(system as int64) as system,  -- noqa: RF04
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
