
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__company`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_company`
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées (dont rib et brita_*), clés vers des tables non
    -- extraites (tariff, delivery_mode, legal_form, joined_file…), paramétrage de l'ERP (idinstance,
    -- language_code, pricing_mode, readonly…).
    select
        cast(idcompany as int64) as idcompany,
        cast(company_idcompany as int64) as company_idcompany,
        cast(idcompany_type as int64) as idcompany_type,
        cast(iddeposit as int64) as iddeposit,
        cast(idpayment_condition as int64) as idpayment_condition,
        cast(idpayment_type as int64) as idpayment_type,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        code,
        name,
        cast(code_status_record as int64) as code_status_record,
        comments,
        email,

        -- Identifiants légaux : espaces de saisie retirés pour les rapprocher d'autres sources.
        nullif(replace(siren, ' ', ''), '') as siren,
        nullif(replace(siret, ' ', ''), '') as siret,
        nullif(replace(tax_number, ' ', ''), '') as tax_number,

        cast(ht_ttc as int64) as ht_ttc,
        cast(vat_class as int64) as vat_class,
        cast(vat_regime as int64) as vat_regime,
        cast(idcountry as int64) as idcountry,
        cast(isparticulier as int64) as isparticulier,
        cast(isassociation as int64) as isassociation,
        cast(balance as numeric) as balance,

        -- Facturation des redevances, quand elle diffère de la facturation courante.
        cast(idpayment_type_facredev as int64) as idpayment_type_facredev,
        cast(idpayment_condition_facredev as int64) as idpayment_condition_facredev,
        cast(ht_ttc_facredev as int64) as ht_ttc_facredev,
        cast(vat_class_facredev as int64) as vat_class_facredev,
        cast(idcountry_facredev as int64) as idcountry_facredev,

        -- Champs personnalisés de l'instance, stockés en XML. EFFECTIF vaut NC quand il n'est pas
        -- communiqué ; la date est saisie en jj/mm/aaaa.
        safe_cast(nullif(regexp_extract(xml, r'<EFFECTIF>([^<]*)</EFFECTIF>'), 'NC') as int64) as effectif,
        safe.parse_date('%d/%m/%Y', regexp_extract(xml, r'<INSTALLATION>([^<]*)</INSTALLATION>'))
            as installation_date,

        -- Dates calendaires : toujours à minuit dans la source.
        date(contract_end_date) as contract_end_date,
        date(lost_date) as lost_date,

        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  