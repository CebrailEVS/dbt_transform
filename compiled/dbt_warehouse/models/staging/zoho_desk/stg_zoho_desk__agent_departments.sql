

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_agents__associated_department_ids`
),

renamed as (
    select
        -- dlt internal pk
        _dlt_id,

        -- fk to stg_zoho_desk__agents (jointure via _dlt_id, pas via agent_id)
        _dlt_parent_id,

        -- fk to stg_zoho_desk__departments (c'est un ID Zoho métier)
        value as department_id,

        -- position dans le tableau JSON d'origine (utile pour le debug)
        _dlt_list_idx

    from source
)

select * from renamed