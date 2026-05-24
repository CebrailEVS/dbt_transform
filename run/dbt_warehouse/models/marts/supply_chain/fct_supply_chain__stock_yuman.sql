
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
      
    partition by stock_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock journalier des pi\u00e8ces d\u00e9tach\u00e9es Yuman, par technicien et par d\u00e9p\u00f4t.\n[COMMENT CONSTRUITE] Lecture directe de stg_yuman_gcs__stock_theorique (export GCS quotidien Yuman), avec conversion export_date \u2192 DATE et filtre sur reference et nom_du_stock non NULL.\n[GRAIN] 1 ligne par (reference, stock, stock_date).\n[NOTES] stock = libell\u00e9 du stock physique (technicien ou d\u00e9p\u00f4t Yuman).\n"""
    )
    as (
      

with filtered_stocks as (
    select
        -- Attributs metier
        reference,
        designation,
        nom_du_stock as stock,

        -- Mesure
        quantite,

        -- Date
        date(export_date) as stock_date,

        -- Metadonnees dbt
        current_timestamp() as dbt_updated_at,
        '1abdb54c-d7d1-4f0f-b172-35eef6fbcb0c' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks
    );
  