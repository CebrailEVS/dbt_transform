{{ config(materialized='table') }}

-- Historique des ruptures TechCare, partie détaillée : seulement les jours où le dépôt
-- est à zéro (qty_depot <= 0), les seuls qui peuvent compter en rupture (règle
-- qualifier_rupture_tech de l'app : stock dépôt > 0 → jamais en rupture). Les autres
-- jours ne servent qu'au dénominateur du taux : ils sont comptés dans
-- app_cockpit__technique_rupture_depot_mois. Depuis le 1er janvier N-1 (fenêtre de
-- l'app), références de fournisseur connu, fournisseur et code article calculés comme
-- l'app (préfixes, sensible à la casse).
with lignes as (
    select
        stock_date,
        depot,
        trim(reference) as reference,
        coalesce(qty_depot, 0) as qty_depot,
        coalesce(qty_vans_depot, 0) as qty_vans_depot,
        coalesce(is_out_of_stock_depot, false) as is_out_of_stock_depot
    from {{ ref('fct_supply_chain__rupture_depot_yuman') }}
    where stock_date >= date_trunc(date_sub(current_date('Europe/Paris'), interval 1 year), year)
),
normalisees as (
    select
        *,
        {{ cockpit_fournisseur_rupture_tech('reference') }} as fournisseur
    from lignes
)
select
    stock_date,
    depot,
    reference,
    fournisseur,
    {{ cockpit_code_article_rupture_tech('reference', 'fournisseur') }} as code_article,
    qty_depot,
    qty_vans_depot,
    is_out_of_stock_depot
from normalisees
where fournisseur is not null and qty_depot <= 0
