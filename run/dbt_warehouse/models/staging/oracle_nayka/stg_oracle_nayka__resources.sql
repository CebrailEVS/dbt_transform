
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__resources`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_resources`
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées (dont les identifiants d'agenda), xml (balise vide),
    -- idinstance (constante).
    select
        cast(idresources as int64) as idresources,
        cast(idresources_type as int64) as idresources_type,
        cast(idcontact as int64) as idcontact,
        cast(idcompany as int64) as idcompany,
        cast(idcompany_storehouse as int64) as idcompany_storehouse,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,
        code,
        name,
        cast(code_status_record as int64) as code_status_record,
        cast(dtl_hidden as int64) as dtl_hidden,
        cast(base_rate as numeric) as base_rate,
        timestamp(datetime(arrival), 'Europe/Paris') as arrival,
        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  