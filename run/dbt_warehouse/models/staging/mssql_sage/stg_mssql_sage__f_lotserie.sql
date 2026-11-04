
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
      
    partition by timestamp_trunc(extracted_at, day)
    cluster by ar_ref, de_no

    
    OPTIONS(
      description="""Photos des lots et num\u00e9ros de s\u00e9rie Sage Nunshen (dbo_f_lotserie). Chargement append : chaque run dlt ajoute une photo compl\u00e8te, identifi\u00e9e par extracted_at. Toutes les photos sont gard\u00e9es, la s\u00e9lection d'une photo par jour se fait en aval. Dans une photo, une ligne par mouvement de lot : l'entr\u00e9e (dl_no_in) et, une fois sorti, la sortie (dl_no_out, 0 tant que le lot n'est pas sorti). Sage ne d\u00e9clare aucune cl\u00e9 unique. Sage \u00e9crit 1753-01-01 pour une date vide : convertie en NULL."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_lotserie`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Lot et article
        ar_ref,
        ls_no_serie,
        de_no,

        -- Mouvements (lignes de document)
        dl_no_in,
        dl_no_out,
        ls_mvt_stock,

        -- Quantités
        ls_qte,
        ls_qte_restant,
        ls_qte_res,
        ls_lot_epuise,

        -- Dates
        nullif(ls_fabrication, timestamp('1753-01-01')) as ls_fabrication,
        nullif(ls_peremption, timestamp('1753-01-01')) as ls_peremption,

        -- Libre
        nullif(trim(ls_complement), '') as ls_complement,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
    );
  