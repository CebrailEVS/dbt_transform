
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_yuman_gcs__stock_articles`
      
    
    

    
    OPTIONS(
      description="""Table marts des suivi inventaire th\u00e9orique des pi\u00e8ces/articles Yuman depuis la table staging afin de suivre l'\u00e9volution des stocks des techniciens et d\u00e9p\u00f4ts Yuman"""
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
        '2c49c569-b4fc-4f29-9309-0a459bd137af' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select *
from filtered_stocks
    );
  