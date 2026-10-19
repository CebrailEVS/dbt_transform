{{ config(materialized='table') }}

-- Référentiel des sociétés Neshu (clients, fournisseurs, dépôts) pour Cockpit Supply, construit depuis le staging.
-- Périmètre : types 1, 2, 4 et 6, hors ghost-deletes ERP. Les dépôts (remise en état, stock, logistique
-- interne) se reconnaissent au seul type numérique 6.
select
    idcompany as company_id,
    code as company_code,
    name as company_name,
    idcompany_type = 6 as is_depot
from {{ ref('stg_oracle_neshu__company') }}
where
    idcompany_type in (1, 2, 4, 6)
    and code_status_record <> -1
