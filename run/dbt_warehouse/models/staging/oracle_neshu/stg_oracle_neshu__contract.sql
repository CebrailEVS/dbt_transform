
  
    

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
        _extracted_at
    from `evs-datastack-prod`.`prod_raw`.`evs_contract`
),

-- Le XML de CONTRACT etait parse par un script Python separe
-- (ingest_oracle_evs_contract -> prod_raw.evs_contract_parsed), parce que le tap
-- Meltano ne savait pas lire une colonne XMLTYPE et la laissait NULL. dlt la charge
-- en texte, donc le parsing revient ici : un job, une table et une image en moins.
--
-- Les entites XML doivent etre decodees : ElementTree le faisait, une regex non.
-- Sur les 306 contrats, un seul contient `&apos;` — sans ce decodage la valeur
-- differait de celle du script. `&amp;` est traite EN DERNIER, sinon `&amp;apos;`
-- deviendrait `'` au lieu de `&apos;`.
--
-- Equivalence verifiee le 2026-08-01 contre evs_contract_parsed : 306/306 lignes
-- identiques sur nombre_collab ET engagement.
extraction_xml as (
    select
        idcontract,
        regexp_extract(xml, r'<NOMBRE_COLLAB>([^<]*)</NOMBRE_COLLAB>') as nombre_collab_brut,
        regexp_extract(xml, r'<ENGAGEMENT>([^<]*)</ENGAGEMENT>') as engagement_brut
    from source_data
),

decode_xml as (
    select
        idcontract,
        
    replace(replace(replace(replace(replace(
        nombre_collab_brut,
        '&apos;', "'"), '&quot;', '"'), '&lt;', '<'), '&gt;', '>'), '&amp;', '&')
 as nombre_collab,
        
    replace(replace(replace(replace(replace(
        engagement_brut,
        '&apos;', "'"), '&quot;', '"'), '&lt;', '<'), '&gt;', '>'), '&amp;', '&')
 as engagement
    from extraction_xml
),

parsed_data as (
    select
        idcontract,

        -- nombre_collab → conversion en entier
        cast(nullif(trim(nombre_collab), '') as int64) as nombre_collab,

        -- garder la valeur brute telle quelle
        trim(engagement) as engagement_raw,

        -- version nettoyée numérique
        -- nullif protège le cast quand l'extraction ne contient aucun chiffre
        -- (ex. saisie texte libre « PAS D'ENGAGEMENT ») → NULL au lieu de Bad int64
        case
            when upper(trim(engagement)) = 'AUCUN' then 0
            else cast(
                nullif(regexp_replace(regexp_extract(engagement, r'[\d\s]+'), r'\s+', ''), '') as int64
            )
        end as engagement_clean

    from decode_xml
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
        -- Colonnes extraites du XML (cf. CTE extraction_xml)
        p.nombre_collab,
        p.engagement_raw,
        p.engagement_clean,

        timestamp(c.original_start_date) as original_start_date,
        timestamp(c.original_end_date) as original_end_date,
        timestamp(c.current_end_date) as current_end_date,
        timestamp(c.termination_date) as termination_date,

        timestamp(c.creation_date) as created_at,
        timestamp(coalesce(c.modification_date, c.creation_date)) as updated_at,
        timestamp(c._extracted_at) as extracted_at

    from source_data as c
    left join parsed_data as p on c.idcontract = p.idcontract
)

select * from cleaned_data
    );
  