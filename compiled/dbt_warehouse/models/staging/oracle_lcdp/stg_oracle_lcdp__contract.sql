

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
    from `evs-datastack-prod`.`prod_raw`.`lcdp_contract`
),

cleaned_data as (
    select
        cast(idcontract as int64) as idcontract,
        cast(idcontract_type as int64) as idcontract_type,
        cast(idcompany_self as int64) as idcompany_self,
        cast(idcompany_financial as int64) as idcompany_financial,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        code,
        name,
        code_status_record,

        timestamp(original_start_date) as original_start_date,
        timestamp(original_end_date) as original_end_date,
        timestamp(current_end_date) as current_end_date,
        timestamp(termination_date) as termination_date,

        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data