
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__client`
      
    
    

    
    OPTIONS(
      description="""Table des bases clients Nespresso EVS nettoy\u00e9e et filtr\u00e9e sur les colonnes utiles."""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`nespresso_base_client`
    where third is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by third
            -- Date de MODIFICATION du classeur, et non l'heure de chargement :
            -- plusieurs dépôts arrivent souvent dans un même run (le pipeline
            -- est manuel), et 8 916 clients diffèrent entre deux versions
            -- consécutives. C'est aussi le `dedup_sort` du merge côté dlt.
            order by _fichier_modifie_le desc
        ) as rn
    from source_data

),

base_client as (

    select
        -- ids convertis en bigint
        cast(third as int64) as third,

        -- colonnes texte
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

        -- dates
        safe_cast(club_dt_disp as timestamp) as club_dt_disp,
        safe_cast(last_caps_ord_dt_disp as timestamp) as last_caps_ord_dt_disp,

        -- mesures. Le classeur atterrit ENTIÈREMENT EN TEXTE côté dlt : laisser
        -- dlt inférer les types d'un Excel lui fait créer des colonnes variantes
        -- selon l'ordre des lignes du fichier — 61 % des valeurs de `ns_n_1_ytd`
        -- étaient parties dans `ns_n_1_ytd__v_double` au premier essai. D'où les
        -- casts, y compris sur les quatre montants qui arrivaient typés avant.
        safe_cast(ns as float64) as ns,
        safe_cast(ns_n_1 as float64) as ns_n1,
        safe_cast(ns_n_ytd as float64) as ns_n_ytd,
        safe_cast(ns_n_1_ytd as float64) as ns_n1_ytd,
        cast(caps as int64) as caps,
        cast(caps_n_1 as int64) as caps_n1,
        cast(caps_n_ytd as int64) as caps_n_ytd,
        cast(caps_n_1_ytd as int64) as caps_n1_ytd,
        cast(caps_b2_b as int64) as caps_b2b,
        cast(caps_b2_c as int64) as caps_b2c,
        cast(caps_b2_c_ytd as int64) as caps_b2c_ytd,
        cast(ez as int64) as ez,
        cast(ez_n_ytd as int64) as ez_n_ytd,
        cast(ez_n_1 as int64) as ez_n1,

        -- metadata dlt. Les `_smart_source_*` de tap-spreadsheets-anywhere et
        -- les `_sdc_*` de Singer n'existent plus ; `_fichier_modifie_le` porte
        -- la date du classeur d'où vient la ligne.
        _extracted_at,
        _fichier_modifie_le

    from deduped
    where rn = 1

)

select *
from base_client
    );
  