{{ config(materialized='table') }}

-- Référentiel des sociétés Cafés du Phare pour Cockpit Supply, construit depuis le staging.
-- Périmètre : fournisseurs (1), clients (2), société interne (4) et dépôts (6).
select
    idcompany as company_id,
    code as company_code,
    name as company_name
from {{ ref('stg_oracle_lcdp__company') }}
where idcompany_type in (1, 2, 4, 6)
