
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
      
    partition by timestamp_trunc(do_date, day)
    

    
    OPTIONS(
      description="""Table des docligne Nunshen nettoy\u00e9e et transform\u00e9e depuis la table source dbo_f_docligne de MSSQL Sage"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_docligne`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Identifiant de la ligne de document (clé métier)
        dl_no,
        cb_co_no as cbco_no, -- FK pour table collaborateur

        -- Domaine et type Sage (filtrage métier en aval)
        -- DO_Domaine : 0 = Ventes, 1 = Achats/autre, 2 = Stock interne
        do_domaine,
        do_type,

        -- Champs principaux
        ct_num,
        do_piece,
        dl_design,
        ar_ref,

        -- Dates & montants
        do_date,
        dl_qte,
        dl_montant_ht,
        dl_montant_ttc,
        dl_prix_unitaire,
        dl_valorise,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at
    from source_data
)

select *
from cleaned_data
qualify row_number() over (
    partition by dl_no
    order by updated_at desc, cb_marq desc
) = 1
    );
  