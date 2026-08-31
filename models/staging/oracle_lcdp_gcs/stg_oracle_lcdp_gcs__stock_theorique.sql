{{
    config(
        materialized = 'table',
        partition_by = {"field": "date_system", "data_type": "timestamp"},
        description = 'Table des stocks théoriques Oracle LCDP, chargée par le pipeline dlt oracle_lcdp_stock. Les horloges Oracle, nues, sont déclarées en Europe/Paris.'
    )
}}

select
    cast(id_entity as int64) as id_entity,
    lower(entity_name) as entity_name,
    lower(entity_type) as entity_type,
    -- SYSDATE Oracle est une horloge murale SANS fuseau, et le serveur est en
    -- Europe/Paris. Un cast direct la ferait lire en UTC par BigQuery : l'instant
    -- serait faux de 1 h l'hiver et de 2 h l'été, et toute conversion en heure
    -- locale basculerait alors sur le LENDEMAIN — le batch tourne à 23:15. On
    -- déclare donc le fuseau d'origine. Vérifié sur les 87 journées chargées :
    -- l'écart entre date_system et extracted_at tombe de 1-2 h à moins de 2 min.
    -- Le jour métier reste `snapshot_date` : lui ne porte ni heure ni fuseau.
    timestamp(datetime(date_system), 'Europe/Paris') as date_system,
    resources_code,
    code_source as product_code,
    code_name as product_name,
    -- Même raison : VARCHAR2 Oracle saisi en heure de Paris, à parser comme tel.
    safe.parse_timestamp('%d/%m/%Y %H:%M', date_inventaire, 'Europe/Paris') as date_inventaire,
    cast(stock_inventaire as numeric) as stock_inventaire,
    cast(plus as numeric) as plus,
    cast(moins as numeric) as moins,
    cast(stock_at_date as numeric) as stock_at_date,
    cast(dpa as numeric) as dpa,
    cast(pump as numeric) as pump,
    cast(purchase_price as numeric) as purchase_price,
    _extracted_at as extracted_at,
    snapshot_date
from {{ source('oracle_lcdp_gcs', 'oracle_lcdp_stock_theorique') }}
