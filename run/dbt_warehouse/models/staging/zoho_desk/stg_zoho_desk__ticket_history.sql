
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history`
      
    
    

    
    OPTIONS(
      description="""Journal d'audit par ticket nettoy\u00e9 \u2014 source de v\u00e9rit\u00e9 pour les m\u00e9triques temporelles. Source : prod_raw.zoho_desk_ticket_history Transformation : aucune (pas de colonne id \u00e0 renommer \u2014 la PK est _dlt_id). Le d\u00e9tail des champs modifi\u00e9s est dans stg_zoho_desk__ticket_history_event_info (jointure : _dlt_parent_id = _dlt_id de ce mod\u00e8le).\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_history`
),

renamed as (
    select
        -- primary key (dlt internal) — jointure vers ticket_history_event_info._dlt_parent_id
        _dlt_id,

        -- foreign key
        _zoho_desk_associated_tickets_id,

        -- event (event_name et event_time restent ensemble — ils définissent l'événement)
        event_name,
        event_time,

        -- source
        source,

        -- actor
        actor__id,
        actor__name,
        actor__type

    from source
)

select * from renamed
    );
  