{{
    config(
        materialized='table',
        description="Références article par fournisseur Sage Nunshen (dbo_f_artfourniss) : une ligne par couple article × fournisseur, avec la référence et le tarif d'achat chez ce fournisseur. Un article peut avoir plusieurs fournisseurs, dont un seul principal (af_principal = 1). Sage écrit 1753-01-01 pour une date vide : convertie en NULL."
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_artfourniss') }}
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier : article × fournisseur
        ar_ref,
        ct_num,
        af_principal,

        -- Référence chez le fournisseur
        nullif(trim(af_ref_fourniss), '') as af_ref_fourniss,
        nullif(trim(af_code_barre), '') as af_code_barre,

        -- Tarif d'achat en vigueur
        af_prix_ach,
        af_remise,
        af_type_rem,
        af_prix_dev,
        af_devise,

        -- Nouveau tarif et sa date d'application
        af_prix_ach_nouv,
        af_prix_dev_nouv,
        af_remise_nouv,
        nullif(af_date_application, timestamp('1753-01-01')) as af_date_application,

        -- Conditionnement et approvisionnement
        af_unite,
        af_conversion,
        af_conv_div,
        af_colisage,
        af_qte_mini,
        af_qte_mont,
        af_delai_appro,
        af_garantie,
        eg_champ,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
