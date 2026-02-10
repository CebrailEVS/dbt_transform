
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__contract`
      
    
    

    
    OPTIONS(
      description="""Contrats transform\u00e9s et nettoy\u00e9s depuis la base Oracle"""
    )
    as (
      

with source_data as (
    select
        cast(idcontract as int64) as idcontract,
        idcontract_type,
        idcompany_self,
        idcompany_financial,
        idcompany_peer,
        idcontact_creation,
        idcontact_modification,
        code,
        name,
        code_status_record,
        xml,
        original_start_date,
        original_end_date,
        current_end_date,
        termination_date,
        creation_date,
        modification_date,
        _sdc_extracted_at,
        _sdc_deleted_at
    from `evs-datastack-prod`.`prod_raw`.`evs_contract`
),

parsed_data as (
    select
        cast(idcontract as int64) as idcontract,

        -- nombre_collab → conversion en entier
        cast(nullif(trim(nombre_collab), '') as int64) as nombre_collab,

        -- garder la valeur brute telle quelle
        trim(engagement) as engagement_raw,

        -- version nettoyée numérique
        case
            when upper(trim(engagement)) = 'AUCUN' then 0
            else cast(
                regexp_replace(regexp_extract(engagement, r'[\d\s]+'), r'\s+', '') as int64
            )
        end as engagement_clean,

        timestamp(extracted_at) as parsed_extracted_at
    from `evs-datastack-prod`.`prod_raw`.`evs_contract_parsed`
),

cleaned_data as (
    select
        -- Colonnes de evs_contract
        cast(c.idcontract as int64) as idcontract,
        cast(c.idcontract_type as int64) as idcontract_type,
        cast(c.idcompany_self as int64) as idcompany_self,
        cast(c.idcompany_financial as int64) as idcompany_financial,
        cast(c.idcompany_peer as int64) as idcompany_peer,
        cast(c.idcontact_creation as int64) as idcontact_creation,
        cast(c.idcontact_modification as int64) as idcontact_modification,

        c.code,
        c.name,
        c.code_status_record,
        -- Colonnes enrichies depuis evs_contract_parsed
        p.nombre_collab,
        p.engagement_raw,
        p.engagement_clean,

        timestamp(c.original_start_date) as original_start_date,
        timestamp(c.original_end_date) as original_end_date,
        timestamp(c.current_end_date) as current_end_date,
        timestamp(c.termination_date) as termination_date,

        timestamp(p.parsed_extracted_at) as parsed_extracted_at,
        timestamp(c.creation_date) as created_at,
        timestamp(coalesce(c.modification_date, c.creation_date)) as updated_at,
        timestamp(c._sdc_extracted_at) as extracted_at,
        timestamp(c._sdc_deleted_at) as deleted_at

    from source_data as c
    left join parsed_data as p on c.idcontract = p.idcontract
)

select * from cleaned_data
    );
  