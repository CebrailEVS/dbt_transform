

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

        -- Les TROIS valeurs scalaires ci-dessous sont POLYMORPHES : l'API rend
        -- tantôt du texte ('Closed'), tantôt un horodatage (property_name
        -- 'Due Date'), tantôt un booléen. Le cast explicite en string rend ce
        -- modèle indépendant du type inféré au raw — sans lui, une inférence à
        -- `timestamp` casse les modèles d'intermediate qui les coalescent
        -- (COALESCE(TIMESTAMP, STRING)), comme mesuré le 2026-08-05.

        -- scalar value (quand la valeur n'est pas un before/after — ex : première assignation)
        cast(property_value as string) as property_value,
        property_value__id,
        property_value__name,
        property_value__type,

        -- valeur AVANT modification
        -- scalaire : property_value__previous_value (ex : 'Open')
        -- objet    : property_value__previous_value__id / __name / __type (ex : agent précédent)
        cast(property_value__previous_value as string) as property_value__previous_value,
        property_value__previous_value__id,
        property_value__previous_value__name,
        property_value__previous_value__type,

        -- valeur APRÈS modification
        -- scalaire : property_value__updated_value (ex : 'Closed')
        -- objet    : property_value__updated_value__id / __name / __type (ex : nouvel agent)
        cast(property_value__updated_value as string) as property_value__updated_value,
        property_value__updated_value__id,
        property_value__updated_value__name,
        property_value__updated_value__type,

        -- metadata
        _dlt_list_idx

    from source
)

select * from renamed