
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_company`
      
    
    

    
    OPTIONS(
      description="""\u00c9tiquettes des soci\u00e9t\u00e9s Nayka (vue Oracle v_label_company) : une ligne par soci\u00e9t\u00e9 \u00d7 \u00e9tiquette, avec la famille et le libell\u00e9 fran\u00e7ais, seule source de libell\u00e9 lisible des \u00e9tiquettes."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_v_label_company`
),

cleaned_data as (
    select
        cast(idcompany as int64) as idcompany,
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,
        cast(l_idstring as int64) as l_idstring,
        cast(lf_idstring as int64) as lf_idstring,
        lf_code,
        -- trim : codes et libellés servent de clés aux pivots en aval, et la saisie laisse des espaces.
        trim(l_code) as l_code,
        trim(l_text_fr) as l_text_fr,
        cast(l_enabled as int64) as l_enabled,
        cast(l_isdefault as int64) as l_isdefault,
        cast(l_system as int64) as l_system,
        cast(lf_exclus as int64) as lf_exclus,
        cast(lf_required as int64) as lf_required,
        cast(lf_system as int64) as lf_system,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  