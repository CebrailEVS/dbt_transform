

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_contact_has_device`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcontact as int64) as idcontact,
        cast(iddevice as int64) as iddevice,

        -- Timestamps harmonisés
        -- La source ne porte ni creation_date ni modification_date : pas de
        -- created_at / updated_at exposables (cf. autres tables de jonction).
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data