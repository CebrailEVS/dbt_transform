{{
    config(
        materialized='table',
        description="Familles d'articles Sage Nunshen (dbo_f_famille) : une famille par ligne, avec son libellé et ses paramètres de gestion hérités par les articles. Toutes les familles sont de détail (fa_type = 0) : fa_code_famille est unique à lui seul. Toutes les colonnes métier sont exposées ; seules les copies d'index Sage (cb_<col> en BYTES) et les colonnes techniques cb_* sont écartées."
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_famille') }}
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier : unique dans Sage avec fa_type, et seule ici (fa_type toujours 0)
        fa_code_famille,
        fa_type,
        nullif(trim(fa_intitule), '') as fa_intitule,

        -- Gestion (valeurs par défaut héritées par les articles de la famille)
        fa_suivi_stock,
        fa_nature,
        fa_unite_ven,
        fa_unite_poids,
        fa_coef,
        fa_garantie,
        fa_delai,
        fa_escompte,
        fa_hors_stat,
        fa_vte_debit,
        fa_not_imp,
        fa_contremarque,
        fa_fact_poids,
        fa_fact_forfait,
        fa_publie,
        fa_nb_colis,
        fa_sous_traitance,
        fa_fictif,
        fa_criticite,

        -- Centralisation, statistiques, fiscalité
        nullif(trim(fa_central), '') as fa_central,
        nullif(trim(fa_stat01), '') as fa_stat01,
        nullif(trim(fa_stat02), '') as fa_stat02,
        nullif(trim(fa_stat03), '') as fa_stat03,
        nullif(trim(fa_stat04), '') as fa_stat04,
        nullif(trim(fa_stat05), '') as fa_stat05,
        nullif(trim(fa_code_fiscal), '') as fa_code_fiscal,
        nullif(trim(fa_pays), '') as fa_pays,

        -- Racines de codification des articles
        nullif(trim(fa_racine_ref), '') as fa_racine_ref,
        nullif(trim(fa_racine_cb), '') as fa_racine_cb,

        -- Catalogue (4 niveaux)
        cl_no1,
        cl_no2,
        cl_no3,
        cl_no4,

        -- Frais d'approche (3 frais × 3 remises)
        nullif(trim(fa_frais01_fr_denomination), '') as fa_frais01_fr_denomination,
        fa_frais01_fr_rem01_rem_valeur,
        fa_frais01_fr_rem01_rem_type,
        fa_frais01_fr_rem02_rem_valeur,
        fa_frais01_fr_rem02_rem_type,
        fa_frais01_fr_rem03_rem_valeur,
        fa_frais01_fr_rem03_rem_type,
        nullif(trim(fa_frais02_fr_denomination), '') as fa_frais02_fr_denomination,
        fa_frais02_fr_rem01_rem_valeur,
        fa_frais02_fr_rem01_rem_type,
        fa_frais02_fr_rem02_rem_valeur,
        fa_frais02_fr_rem02_rem_type,
        fa_frais02_fr_rem03_rem_valeur,
        fa_frais02_fr_rem03_rem_type,
        nullif(trim(fa_frais03_fr_denomination), '') as fa_frais03_fr_denomination,
        fa_frais03_fr_rem01_rem_valeur,
        fa_frais03_fr_rem01_rem_type,
        fa_frais03_fr_rem02_rem_valeur,
        fa_frais03_fr_rem02_rem_type,
        fa_frais03_fr_rem03_rem_valeur,
        fa_frais03_fr_rem03_rem_type,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
