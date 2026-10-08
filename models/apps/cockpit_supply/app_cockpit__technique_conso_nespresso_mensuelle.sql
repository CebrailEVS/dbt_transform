{{ config(materialized='table') }}

-- Pièces Nespresso posées par les techniciens, par mois, référence et technicien : grain
-- de tous les calculs mensuels de l'app (conso par dépôt, par technicien, index
-- saisonnier, facturation). Filtres repris de l'app (data/csv_source.py) :
-- interventions réalisées, date renseignée, codes de main-d'œuvre exclus. Le mois est
-- celui de date_heure_debut en UTC, comme l'app. Le technicien → dépôt / nom et le
-- prix restent calculés par l'app (sources hors BigQuery).
select
    format_timestamp('%Y-%m', date_heure_debut) as mois,
    coalesce(upper(trim(product_reference)), 'NONE') as product_reference,
    technician_id,
    coalesce(sum(qty_consommee), 0) as qte,
    max(nom_article) as nom_article
from {{ ref('fct_technique__consommation_article_nespresso') }}
where
    etat_intervention in ('terminée signée', 'terminée non signée')
    and date_heure_debut is not null
    and coalesce(upper(trim(code_article)), 'NONE') not in ('0000001', '0000100')
group by mois, product_reference, technician_id
