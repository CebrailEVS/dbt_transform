{{
    config(
        materialized='table',
        description='Détail des champs modifiés par événement de ticket_history — une ligne par champ modifié. Jointure : _dlt_parent_id = stg_zoho_desk__ticket_history._dlt_id. Filtrer sur property_name pour isoler un type de changement (Status, Assignee, Priority, etc.).'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_ticket_history__event_info') }}
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
