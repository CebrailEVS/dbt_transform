
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
      
    partition by timestamp_trunc(created_at, day)
    cluster by ec_no

    
    OPTIONS(
      description="""\u00c9critures analytiques nettoy\u00e9es issues du syst\u00e8me MSSQL Sage. D\u00e9duplication staging sur (ec_no, n_analytique, ea_ligne) \u2014 cl\u00e9 unique m\u00e9tier Sage \u2014 en gardant le cb_marq le plus r\u00e9cent."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_ecriturea`
),

cleaned_data as (
    select
        -- IDs et numéros (colonnes désormais INT64 natifs)
        cb_marq,
        ec_no,
        n_analytique,
        ea_ligne,

        -- Colonnes texte
        ca_num,
        cb_createur,
        cb_creation_user,

        -- Colonnes numériques avec gestion des valeurs nulles
        ea_montant,
        ea_quantite,

        -- Timestamps harmonisés (colonnes désormais TIMESTAMP natifs)
        cb_creation as created_at,
        -- Fallback to cb_creation when cb_modification is null
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at

    from source_data
)

select *
from cleaned_data
qualify row_number() over (
    partition by ec_no, n_analytique, ea_ligne
    order by updated_at desc, cb_marq desc
) = 1
    );
  