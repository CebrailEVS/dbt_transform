
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history_event_info`
      
    
    

    
    OPTIONS(
      description="""D\u00e9tail des champs modifi\u00e9s lors d'un \u00e9v\u00e9nement de ticket_history. Source : prod_raw.zoho_desk_ticket_history__event_info Transformation : aucune (pas de colonne id \u00e0 renommer \u2014 la PK est _dlt_id). Jointure vers ticket_history : _dlt_parent_id = stg_zoho_desk__ticket_history._dlt_id Usage typique :\n  Filtrer sur property_name = 'Status' pour les transitions de statut.\n  Filtrer sur property_name = 'Assignee' pour les changements d'assign\u00e9.\n  Les valeurs avant/apr\u00e8s sont dans property_value__previous_value /\n  property_value__updated_value (scalaires) ou dans les colonnes __id/__name/__type\n  (quand la valeur est un objet Zoho comme un agent ou d\u00e9partement).\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_history__event_info`
),

renamed as (
    select
        -- primary key (dlt internal)
        _dlt_id,

        -- foreign key to stg_zoho_desk__ticket_history (dlt internal)
        _dlt_parent_id,

        -- property metadata
        property_name,
        property_type,
        system_property,

        -- scalar value (quand la valeur n'est pas un before/after — ex : première assignation)
        property_value,
        property_value__id,
        property_value__name,
        property_value__type,

        -- valeur AVANT modification
        -- scalaire : property_value__previous_value (ex : 'Open')
        -- objet    : property_value__previous_value__id / __name / __type (ex : agent précédent)
        property_value__previous_value,
        property_value__previous_value__id,
        property_value__previous_value__name,
        property_value__previous_value__type,

        -- valeur APRÈS modification
        -- scalaire : property_value__updated_value (ex : 'Closed')
        -- objet    : property_value__updated_value__id / __name / __type (ex : nouvel agent)
        property_value__updated_value,
        property_value__updated_value__id,
        property_value__updated_value__name,
        property_value__updated_value__type,

        -- metadata
        _dlt_list_idx

    from source
)

select * from renamed
    );
  