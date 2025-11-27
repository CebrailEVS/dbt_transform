{{ config(
    materialized='table',
    partition_by = {
    'field': 'date_system',
    'data_type': 'timestamp'},
    description= 'Table de fait marts afin de suivre l"inventaire véhicule roadman + dépôt filtrer sur vehicule actif et depôt 1-5 10 et 13, enrichi et cleané'
) }}

WITH resources_labels AS (
    SELECT
        r.idresources,
        r.idresources_type,
        r.code AS resources_code,
        r.name AS resources_name,
        lh.idlabel,
        l.code AS label_code,
        lf.code AS label_family_code
    FROM {{ ref('stg_oracle_neshu__resources') }} r
    LEFT JOIN {{ ref('stg_oracle_neshu__label_has_resources') }} lh
        ON lh.idresources = r.idresources
    LEFT JOIN {{ ref('stg_oracle_neshu__label') }} l
        ON l.idlabel = lh.idlabel
    LEFT JOIN {{ ref('stg_oracle_neshu__label_family') }} lf
        ON lf.idlabel_family = l.idlabel_family
),

aggregated_labels AS (
    SELECT
        idresources,
        idresources_type,
        resources_code,
        resources_name,
        MAX(CASE WHEN label_family_code = 'ISACTIVE' THEN label_code END) AS is_active
    FROM resources_labels
    WHERE idresources_type = 3  -- TYPE VEHICULE
    GROUP BY 1,2,3,4
),

filtered_stock AS (
    SELECT
        st.id_entity,
        st.entity_type,
        st.resources_code AS entity_code,
        st.entity_name,
        st.product_code,
        st.product_name,
        st.stock_at_date,
        CASE WHEN st.stock_at_date = 0 THEN TRUE ELSE FALSE END AS is_out_of_stock,
        st.date_inventaire,
        st.stock_inventaire,
        st.plus,
        st.moins,
        COALESCE(st.dpa, st.purchase_price) AS dpa,
        st.purchase_price,
        st.date_system,
        st.extracted_at,
        st.file_datetime
    FROM {{ ref('stg_oracle_neshu_gcs__stock_theorique') }} st
    LEFT JOIN aggregated_labels al
        ON st.id_entity = al.idresources
        AND st.entity_type = 'RESOURCE'
    WHERE 
        (
            st.entity_type = 'COMPANY'
            AND st.entity_name IN (
                '01 - RUNGIS DEPOT PRODUITS',
                '02 - LYON DEPOT PRODUITS',
                '03 - BORDEAUX DEPOT PRODUITS',
                '04 - STRASBOURG DEPOT PRODUITS',
                '05 - PERIMES DEPOT',
                '10 - REBUS DEPOT',
                '13 - MARSEILLE DEPOT PRODUITS'
            )
        )
        OR
        (
            st.entity_type = 'RESOURCE'
            AND al.is_active = 'YES'
        )
)

SELECT *
FROM filtered_stock
