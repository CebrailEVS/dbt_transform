
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__contract`
      
    
    

    
    OPTIONS(
      description="""Contrats clients Nayka (Oracle contract) : soci\u00e9t\u00e9 signataire et cliente, dur\u00e9e, reconduction, pr\u00e9avis, dates. Les champs personnalis\u00e9s EFFECTIF, ENGAGEMENTBIB et ENGAGEMENTBOISSONS du xml sont extraits et typ\u00e9s."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_contract`
),

cleaned_data as (
    select
        cast(idcontract as int64) as idcontract,
        cast(idcompany_self as int64) as idcompany_self,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idcontact_peer_signature as int64) as idcontact_peer_signature,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        code,
        name,
        cast(code_status_record as int64) as code_status_record,

        cast(duration as int64) as duration,
        cast(renewable as int64) as renewable,
        cast(renewable_duration as int64) as renewable_duration,
        cast(notice_period as int64) as notice_period,

        -- Dates calendaires : toujours à minuit dans la source.
        date(original_start_date) as original_start_date,
        date(original_end_date) as original_end_date,
        date(current_end_date) as current_end_date,

        -- Champs personnalisés de l'instance, stockés en XML. NC = non communiqué ; AUCUN = pas
        -- d'engagement, soit 0.
        safe_cast(nullif(regexp_extract(xml, r'<EFFECTIF>([^<]*)</EFFECTIF>'), 'NC') as int64) as effectif,
        safe_cast(
            replace(regexp_extract(xml, r'<ENGAGEMENTBIB>([^<]*)</ENGAGEMENTBIB>'), 'AUCUN', '0') as int64
        ) as engagement_bib,
        safe_cast(
            replace(regexp_extract(xml, r'<ENGAGEMENTBOISSONS>([^<]*)</ENGAGEMENTBOISSONS>'), 'AUCUN', '0') as int64
        ) as engagement_boissons,

        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  