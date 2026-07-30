{{
    config(
        materialized='table',
        description='Labels company LCDP nettoyés depuis la vue aplatie lcdp_v_label_company (1 ligne par company/label). Porte le code, la famille et le libellé FR du label, prêt pour le pivot dans dim_lcdp__company.'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_lcdp', 'lcdp_v_label_company') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcompany as int64) as company_id,
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,

        -- Identifiants techniques (string)
        l_idstring as label_idstring,
        lf_idstring as label_family_idstring,

        -- Codes et libellés
        l_code as label_code,
        lf_code as label_family_code,
        l_text_fr as label_text_fr,

        -- Booléens label
        cast(cast(l_system as int64) as boolean) as is_system,
        cast(cast(l_enabled as int64) as boolean) as is_enabled,
        cast(cast(l_isdefault as int64) as boolean) as is_default,

        -- Booléens famille de label
        cast(cast(lf_exclus as int64) as boolean) as is_family_exclusive,
        cast(cast(lf_system as int64) as boolean) as is_family_system,
        cast(cast(lf_required as int64) as boolean) as is_family_required,
        cast(cast(lf_export_mobile as int64) as boolean) as is_family_export_mobile,

        -- Timestamps harmonisés
        -- La vue ne porte aucune date métier (création/modification) : seuls les timestamps techniques sont disponibles
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
