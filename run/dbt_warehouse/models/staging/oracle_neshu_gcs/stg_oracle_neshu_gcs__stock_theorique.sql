
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu_gcs__stock_theorique`
      
    partition by timestamp_trunc(date_system, day)
    cluster by id_entity, resources_code

    
    OPTIONS(
      description="""Table staging des fichiers CSV quotidiens de stock th\u00e9orique Oracle Neshu depuis GCS. Les colonnes sont typ\u00e9es et les dates converties en TIMESTAMP."""
    )
    as (
      

SELECT
    CAST(id_entity AS INT64) AS id_entity,  
    entity_name,
    entity_type,
    CAST(DATE_SYSTEM AS TIMESTAMP) AS date_system,
    resources_code,
    code_source AS product_code,
    code_name AS product_name,
    SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M', DATE_INVENTAIRE) AS date_inventaire,
    CAST(STOCK_INVENTAIRE AS NUMERIC) AS stock_inventaire,
    CAST(PLUS AS NUMERIC) AS plus,
    CAST(MOINS AS NUMERIC) AS moins,
    CAST(STOCK_AT_DATE AS NUMERIC) AS stock_at_date,
    CAST(DPA AS NUMERIC) AS dpa,
    CAST(PUMP AS NUMERIC) AS pump,
    CAST(PURCHASE_PRICE AS NUMERIC) AS purchase_price,
    CAST(extracted_at AS TIMESTAMP) AS extracted_at,
    row_count,
    PARSE_DATETIME('%Y_%m_%d_%H%M', REGEXP_EXTRACT(_FILE_NAME, r'(\d{4}_\d{2}_\d{2}_\d{4})')) AS file_datetime
FROM `evs-datastack-prod`.`prod_raw`.`ext_gcs_oracle_neshu__stock_theorique`
    );
  