{{ config(materialized='table') }}

-- Disponibilité mensuelle des articles dans les dépôts Neshu, pour Cockpit Supply : règle de la macro
-- disponibilite_depot_mensuelle (jour sans ligne = rupture, stock négatif = rupture). Code dépôt du dernier jour.
with disponibilite as (
    {{ disponibilite_depot_mensuelle(ref('int_oracle_neshu__stock_theorique')) }}
)

select
    mois,
    company_id,
    entity_code,
    product_code,
    taux_disponibilite_pct
from disponibilite
