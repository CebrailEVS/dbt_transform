
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_artstock`
      
    partition by timestamp_trunc(extracted_at, day)
    cluster by ar_ref, de_no

    
    OPTIONS(
      description="""Photos du stock Nunshen par article \u00d7 d\u00e9p\u00f4t \u2014 une photo compl\u00e8te par run dlt (extracted_at)."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_artstock`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (unique DANS une photo seulement)
        cb_marq,

        -- Grain : photo (extracted_at) × article × dépôt
        ar_ref,
        de_no,
        as_principal,

        -- Quantités (unité de l'article)
        as_qte_sto,
        as_qte_res,
        as_qte_com,
        as_qte_res_cm,
        as_qte_com_cm,
        as_qte_prepa,
        as_qte_a_controler,
        as_qte_mini,
        as_qte_maxi,

        -- Valorisation
        as_mont_sto,

        -- Emplacements
        dp_no_principal,
        dp_no_controle,
        as_mouvemente,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
    );
  