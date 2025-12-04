{{ 
  config(
    materialized='table',
    description='Table des bases clients Nespresso EVS nettoyée et filtrée sur les colonnes utiles.'
  ) 
}}

with source_data as (
    select *
    from {{ source('nesp_co', 'nespresso_base_client') }}
    where third is not null
),

base_client as (
    select
        -- IDs convertis en BIGINT
        cast(third as int64) as third,

        -- Colonnes texte
        third_name,
        third_adr_ln1,
        third_adr_ln2,
        third_post_code,
        third_city,
        third_status_descr, 
        segmentation_hypercare,
        categorie_client,
        region,
        secteur,
        code_lmb,
        mb_descr,
        code_mb,
        siret,
        nb_salaries,
        order_placer_name,
        order_placer_adr_ln1,
        order_placer_post_code,
        order_placer_city,
        order_placer_phone,

        -- Dates
        safe_cast(club_dt_disp as timestamp) as club_dt_disp,
        safe_cast(last_caps_ord_dt_disp as timestamp) as last_caps_ord_dt_disp,

        -- Mesures
        ns,
        ns_n1,
        ns_n_ytd,
        ns_n1_ytd,
        cast(caps as int64) as caps,
        cast(caps_n1 as int64) as caps_n1,
        cast(_caps_n_ytd as int64) as caps_n_ytd,
        cast(caps_n1_ytd as int64) as caps_n1_ytd,
        cast(caps_b2b as int64) as caps_b2b,
        cast(caps_b2c as int64) as caps_b2c,
        cast(caps_b2c_ytd_ as int64) as caps_b2c_ytd,
        cast(ez as int64) as ez,
        cast(ez_n_ytd as int64) as ez_n_ytd,
        cast(ez_n1 as int64) as ez_n1,

        -- Metadata
        _smart_source_bucket,
        _smart_source_file,
        _smart_source_lineno,
        _sdc_batched_at,
        _sdc_received_at

    from source_data
)

select * from base_client