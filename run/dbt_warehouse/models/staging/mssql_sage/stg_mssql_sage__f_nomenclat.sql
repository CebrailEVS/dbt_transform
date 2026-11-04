
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_nomenclat`
      
    
    

    
    OPTIONS(
      description="""Nomenclatures Sage Nunshen (dbo_f_nomenclat) : une ligne par lien compos\u00e9 \u2192 composant, avec la quantit\u00e9 de composant pour une unit\u00e9 de compos\u00e9. Cl\u00e9 Sage : (ar_ref, no_ref_det, ag_no1, ag_no2, no_operation) ; gammes et op\u00e9rations ne sont pas utilis\u00e9es chez Nunshen, donc (ar_ref, no_ref_det) est unique en pratique."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_nomenclat`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier : composé → composant (+ gammes et opération, inutilisées)
        ar_ref,
        no_ref_det,
        ag_no1,
        ag_no2,
        nullif(trim(no_operation), '') as no_operation,

        -- Composition
        no_qte,
        no_ordre,
        no_type,
        no_repartition,
        ag_no1_comp,
        ag_no2_comp,
        de_no,
        no_sous_traitance,
        nullif(trim(no_commentaire), '') as no_commentaire,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
    );
  