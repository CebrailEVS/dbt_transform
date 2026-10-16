

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_device`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(iddevice as int64) as iddevice,
        cast(device_iddevice as int64) as device_iddevice,
        cast(iddevice_type as int64) as iddevice_type,
        cast(idmodel as int64) as idmodel,
        cast(idcompany_customer as int64) as idcompany_customer,
        cast(idcompany_supplier as int64) as idcompany_supplier,
        cast(idcompany_owner as int64) as idcompany_owner,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,
        cast(idlocation as int64) as idlocation,

        -- Colonnes texte
        code,
        name,
        serial,
        code_status_record,
        nullif(trim(comments), '') as comments,

        -- Date liée à la machine
        timestamp(datetime(last_installation_date), 'Europe/Paris') as last_installation_date,
        date(purchase_date) as purchase_date,

        -- Coût d'acquisition. 0 est fréquent à la source, qui ne distingue pas « non saisi » de « nul ».
        cast(purchase_cost as float64) as purchase_cost,

        -- Timestamps harmonisés
        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data