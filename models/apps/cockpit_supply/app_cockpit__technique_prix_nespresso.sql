{{ config(materialized='table') }}

-- Prix unitaire des pièces Nespresso (système Nomad), un par référence : c'est tout ce
-- que l'app lit de cette table (179 k lignes d'interventions → ~410 références).
-- Un seul prix par référence dans la source (vérifié le 2026-10-08).
select distinct
    piece_ref_nomad,
    piece_prix_unitaire
from {{ ref('fct_technique__piece_detachee_pricing_nespresso') }}
where piece_prix_unitaire is not null
