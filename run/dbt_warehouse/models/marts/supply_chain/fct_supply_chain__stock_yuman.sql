
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
      
    partition by stock_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock journalier des pi\u00e8ces d\u00e9tach\u00e9es Yuman, par technicien et par d\u00e9p\u00f4t.\n[COMMENT CONSTRUITE] Lecture directe de stg_yuman_gcs__stock_theorique (export GCS quotidien Yuman), avec conversion export_date \u2192 DATE et filtre sur reference et nom_du_stock non NULL. type_stock classe chaque emplacement en DEPOT/TECHNICIEN via la macro yuman_stock_type.\n[GRAIN] 1 ligne par (reference, stock, stock_date).\n[NOTES] stock = libell\u00e9 du stock physique (technicien ou d\u00e9p\u00f4t Yuman). Ne contient que des lignes en stock (quantite > 0) : les ruptures (sans emplacement) sont exclues et expos\u00e9es au grain article dans fct_supply_chain__stock_article_yuman.\n"""
    )
    as (
      

with filtered_stocks as (
    select
        -- Attributs metier
        reference,
        designation,
        nom_du_stock as stock,
        case when (nom_du_stock like '%DEPOT%') then 'DEPOT' else 'TECHNICIEN' end as type_stock,

        -- Mesure
        quantite,

        -- Date
        date(export_date) as stock_date,

        -- Metadonnees dbt
        current_timestamp() as dbt_updated_at,
        '797d9f7e-cb98-48ef-a510-093856213dd5' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks
    );
  