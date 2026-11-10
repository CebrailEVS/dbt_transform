{{ config(materialized='table') }}

-- Sorties de fabrication (torréfaction) Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées (FAIT, VALIDE) qui sortent bien de FABRICATION ; une sortie saisie pour un simple transfert
-- est comptée en livraison interne (app_cockpit__lcdp_livraison_interne).
select
    task_product_id,
    task_id,
    product_code,
    {{ utc_vers_paris_affichage('task_start_date') }} as task_start_date,
    source_code,
    destination_code,
    quantity,
    valuation
from {{ ref('int_oracle_lcdp__sortie_fabrication_tasks') }}
where
    task_status_code in ('FAIT', 'VALIDE')
    and not is_transfert
