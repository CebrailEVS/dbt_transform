
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__agent_departments`
      
    
    

    
    OPTIONS(
      description="""Table pont agent \u2194 d\u00e9partement \u2014 d\u00e9normalis\u00e9e depuis le tableau JSON associated_department_ids de l'API Zoho (11 lignes). Source : prod_raw.zoho_desk_agents__associated_department_ids Transformation : aucune (pas de colonne id \u00e0 renommer dans cette table). Jointures :\n  \u2192 agent      : _dlt_parent_id = stg_zoho_desk__agents._dlt_id  (cl\u00e9 dlt interne)\n  \u2192 d\u00e9partement : value = stg_zoho_desk__departments.department_id (ID Zoho m\u00e9tier)\n"""
    )
    as (
      

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
    );
  