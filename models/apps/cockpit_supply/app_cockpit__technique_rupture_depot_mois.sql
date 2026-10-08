{{ config(materialized='table') }}

-- Historique des ruptures TechCare, partie comptée : nombre de jours suivis par mois,
-- dépôt et référence (dénominateur du taux de disponibilité) et dernier jour suivi.
-- Même fenêtre et mêmes règles fournisseur / code article que
-- app_cockpit__technique_rupture_depot_jour_vide.
with normalisees as (
    select
        stock_date,
        depot,
        trim(reference) as reference,
        {{ cockpit_fournisseur_rupture_tech('trim(reference)') }} as fournisseur
    from {{ ref('fct_supply_chain__rupture_depot_yuman') }}
    where stock_date >= date_trunc(date_sub(current_date('Europe/Paris'), interval 1 year), year)
)
select
    format_date('%Y-%m', stock_date) as mois,
    depot,
    fournisseur,
    {{ cockpit_code_article_rupture_tech('reference', 'fournisseur') }} as code_article,
    reference,
    count(*) as nb_jours,
    max(stock_date) as derniere_date
from normalisees
where fournisseur is not null
group by mois, depot, fournisseur, code_article, reference
