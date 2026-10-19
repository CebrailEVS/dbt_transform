{{ config(materialized='table') }}

-- Références connues du stock TechCare sur tout l'historique : l'app s'en sert pour
-- retrouver la référence complète (EVS_NESPRESSO_ / EVS_NESP_) d'un code court.
select distinct reference
from {{ ref('stg_yuman_evs_sftp__stock_theorique') }}
where
    reference is not null
    and nom_du_stock is not null
