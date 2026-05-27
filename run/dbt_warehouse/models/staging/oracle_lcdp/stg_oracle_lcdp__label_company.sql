
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_company`
      
    
    

    
    OPTIONS(
      description="""Labels company nettoy\u00e9s depuis la vue aplatie lcdp_v_label_company (1 ligne par company/label). Porte code, famille et libell\u00e9 FR du label, pr\u00eat pour le pivot dans dim_lcdp__company."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_v_label_company`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcompany as int64) as company_id,
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
    );
  