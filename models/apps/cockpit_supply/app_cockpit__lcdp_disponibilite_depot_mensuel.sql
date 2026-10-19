{{ config(materialized='table') }}

-- Disponibilité mensuelle des articles dans les dépôts Cafés du Phare, pour Cockpit Supply, construite depuis
-- l'intermediate : part des jours observés où l'article n'est pas en rupture. Libellés du dernier jour du mois.
with mensuel as (
    select
        date_trunc(date(date_system), month) as mois,
        id_entity as company_id,
        product_code,
        any_value(entity_code having max date_system) as entity_code,
        count(distinct date(date_system)) as jours_observes,
        count(distinct case when is_out_of_stock = false then date(date_system) end) as jours_disponibles
    from {{ ref('int_oracle_lcdp__stock_theorique') }}
    where entity_type = 'company'
    group by 1, 2, 3
)

select
    mois,
    company_id,
    entity_code,
    product_code,
    round(safe_divide(jours_disponibles, jours_observes) * 100, 1) as taux_disponibilite_pct
from mensuel
