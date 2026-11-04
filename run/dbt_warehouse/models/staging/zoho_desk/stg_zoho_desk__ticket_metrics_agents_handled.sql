
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_metrics_agents_handled`
      
    
    

    
    OPTIONS(
      description="""Agents ayant trait\u00e9 chaque ticket avec leur temps de traitement. Source : prod_raw.zoho_desk_ticket_metrics__agents_handled Transformation : handling_time converti en minutes. Pas de colonne id \u00e0 renommer. Jointure vers ticket_metrics : _dlt_parent_id = stg_zoho_desk__ticket_metrics._dlt_id\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_metrics__agents_handled`
),

renamed as (
    select
        -- primary key (dlt internal)
        _dlt_id,

        -- foreign key to stg_zoho_desk__ticket_metrics (dlt internal)
        _dlt_parent_id,

        -- agent
        agent_id,
        agent_name,

        -- duration (STRING 'HH:MM hrs' → INT64 minutes)
        safe_cast(split(replace(handling_time, ' hrs', ''), ':')[safe_offset(0)] as int64) * 60
        + safe_cast(split(replace(handling_time, ' hrs', ''), ':')[safe_offset(1)] as int64)
            as handling_time_minutes,

        -- metadata
        _dlt_list_idx

    from source
)

select * from renamed
    );
  