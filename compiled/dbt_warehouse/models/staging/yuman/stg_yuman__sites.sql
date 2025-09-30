

with source_data as (
    select * from `evs-datastack-prod`.`prod_raw`.`yuman_sites`
),

extracted_postal_code as (
    select
        *,
        (
            select JSON_EXTRACT_SCALAR(value, '$.value')
            from unnest(JSON_EXTRACT_ARRAY(_embed_fields)) as value
            where JSON_EXTRACT_SCALAR(value, '$.name') = 'CODE POSTAL'
            limit 1
        ) as raw_code_postal
    from source_data
),

cleaned as (
    select
        id as site_id,
        client_id,
        agency_id,
        code as site_code,
        name as site_name,
        address as site_address,
        -- Nettoyage du code postal : suppression du ".0", puis cast en texte
        regexp_replace(raw_code_postal, r'\.0$', '') as site_postal_code,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(_sdc_extracted_at as timestamp) as extracted_at,
        cast(_sdc_deleted_at as timestamp) as deleted_at
    from extracted_postal_code
)

select * from cleaned