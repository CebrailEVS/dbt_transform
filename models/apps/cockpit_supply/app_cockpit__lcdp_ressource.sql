{{ config(materialized='table') }}

-- Référentiel des ressources Cafés du Phare (personnes et véhicules) pour Cockpit Supply, construit depuis le staging.
select
    r.idresources as resources_id,
    r.code as resources_code,
    r.name as resources_name,
    rt.code as resources_type
from {{ ref('stg_oracle_lcdp__resources') }} as r
inner join {{ ref('stg_oracle_lcdp__resources_type') }} as rt
    on r.idresources_type = rt.idresources_type
where rt.code in ('PERSON', 'VEHICLE')
