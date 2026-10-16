

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_device`
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées (dont izly_api_key, idmachine_nayax, idtariff),
    -- xml (balise vide), paramétrage constant de l'ERP (idinstance, audit_param_config_default).
    select
        cast(iddevice as int64) as iddevice,
        cast(device_iddevice as int64) as device_iddevice,
        cast(iddevice_type as int64) as iddevice_type,
        cast(idmodel as int64) as idmodel,
        cast(idcompany_customer as int64) as idcompany_customer,
        cast(idcompany_owner as int64) as idcompany_owner,
        cast(idcompany_supplier as int64) as idcompany_supplier,
        cast(idlocation as int64) as idlocation,
        cast(iddamping_type as int64) as iddamping_type,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        code,
        name,
        serial,
        external_reference,
        comments,
        cast(code_status_record as int64) as code_status_record,

        cast(purchase_cost as numeric) as purchase_cost,
        cast(damping_duration as int64) as damping_duration,

        -- Dates calendaires : à minuit dans la source (une saisie horodatée isolée sur la garantie).
        date(purchase_date) as purchase_date,
        date(start_date_guarantee) as start_date_guarantee,
        timestamp(datetime(last_installation_date), 'Europe/Paris') as last_installation_date,

        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data