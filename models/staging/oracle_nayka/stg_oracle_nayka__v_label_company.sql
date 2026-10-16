{{
    config(
        materialized='table',
        description="Étiquettes des sociétés Nayka (vue Oracle v_label_company) : une ligne par société × étiquette, avec la famille et le libellé français, seule source de libellé lisible des étiquettes."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_v_label_company') }}
),

cleaned_data as (
    -- Non reprises : l_idstring et lf_idstring (table string non extraite), lf_export_mobile (mobile).
    -- Préfixes de la vue : l_ = étiquette, lf_ = famille.
    select
        cast(idcompany as int64) as idcompany,
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,
        lf_code,
        -- trim : codes et libellés servent de clés aux pivots en aval, et la saisie laisse des espaces.
        trim(l_code) as l_code,
        trim(l_text_fr) as l_text_fr,
        cast(l_enabled as int64) as l_enabled,
        cast(l_isdefault as int64) as l_isdefault,
        cast(l_system as int64) as l_system,
        cast(lf_exclus as int64) as lf_exclus,
        cast(lf_required as int64) as lf_required,
        cast(lf_system as int64) as lf_system,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
