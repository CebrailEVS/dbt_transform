
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_metrics_staging_data`
      
    
    

    
    OPTIONS(
      description="""Temps pass\u00e9 par chaque ticket dans chaque statut Zoho. Source : prod_raw.zoho_desk_ticket_metrics__staging_data Transformation : handled_time converti en minutes. Pas de colonne id \u00e0 renommer. Jointure vers ticket_metrics : _dlt_parent_id = stg_zoho_desk__ticket_metrics._dlt_id Note : 'staging' est un terme Zoho (= \u00e9tape de statut), pas un concept dbt.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_metrics__staging_data`
),

renamed as (
    select
        -- primary key (dlt internal)
        _dlt_id,

        -- foreign key to stg_zoho_desk__ticket_metrics (dlt internal)
        _dlt_parent_id,

        -- status stage
        status,

        -- duration (STRING 'HH:MM hrs' → INT64 minutes)
        safe_cast(split(replace(handled_time, ' hrs', ''), ':')[safe_offset(0)] as int64) * 60
        + safe_cast(split(replace(handled_time, ' hrs', ''), ':')[safe_offset(1)] as int64)
            as handled_time_minutes,

        -- metadata
        _dlt_list_idx

    from source
)

select * from renamed
    );
  