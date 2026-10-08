{{ config(materialized='table') }}

-- Référentiel des sociétés Neshu (clients, fournisseurs, dépôts) : copie pour Cockpit Supply de dim_neshu__company
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('dim_neshu__company') }}
