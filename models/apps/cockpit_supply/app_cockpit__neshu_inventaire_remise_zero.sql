{{ config(
    materialized='table',
    cluster_by=['entity_code', 'product_code']
) }}

-- Distrilog n'exporte pas les stocks nuls : un article remis à 0 par un comptage disparaît de la photo au lieu d'y porter
-- sa nouvelle date d'inventaire, et app_cockpit__neshu_inventaire_transition ne le voit pas. Ce sont les disparitions
-- prouvées par une tâche ÉCART INVENTAIRE, rattachées dans app_cockpit__neshu_stock_disparition.
select
    date_system,
    snapshot_date,
    entity_code,
    product_code,
    date_inventaire,
    date_inventaire_precedente,
    stock_veille,
    qte_ecart,
    dpa_veille,
    task_id,
    task_product_id,
    task_status_code,
    nb_lignes_ecart
from {{ ref('app_cockpit__neshu_stock_disparition') }}
where is_remise_inventaire
