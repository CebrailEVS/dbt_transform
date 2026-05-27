

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_v_label_device`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(iddevice as int64) as device_id,
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,

        -- Identifiants techniques (string)
        l_idstring as label_idstring,
        lf_idstring as label_family_idstring,

        -- Codes et libellés
        l_code as label_code,
        lf_code as label_family_code,
        l_text_fr as label_text_fr,

        -- Booléens label
        cast(l_system as boolean) as is_system,
        cast(l_enabled as boolean) as is_enabled,
        cast(l_isdefault as boolean) as is_default,

        -- Booléens famille de label
        cast(lf_exclus as boolean) as is_family_exclusive,
        cast(lf_system as boolean) as is_family_system,
        cast(lf_required as boolean) as is_family_required,
        cast(lf_export_mobile as boolean) as is_family_export_mobile,

        -- Timestamps harmonisés
        -- La vue ne porte aucune date métier (création/modification) : seuls les timestamps techniques sont disponibles
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data