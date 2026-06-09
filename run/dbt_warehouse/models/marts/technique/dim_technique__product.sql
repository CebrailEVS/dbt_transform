
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_technique__product`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nCatalogue des produits / articles Yuman (pi\u00e8ces d\u00e9tach\u00e9es, consommables)\nutilisables lors des interventions. Appel\u00e9 \u00ab Article \u00bb c\u00f4t\u00e9 m\u00e9tier\n(renommage fait dans le mod\u00e8le s\u00e9mantique Power BI).\n\n[COMMENT CONSTRUITE]\nLecture directe de `stg_yuman__products` (catalogue API Yuman), sans\ntransformation.\n\n[GRAIN]\n1 ligne par `product_id` (PK).\n\n[NOTES]\nDimension conforme : sert aux analyses technique (consommation\nd'articles via `fct_technique__workorder_product`) et supply_chain\n(le stock `fct_supply_chain__stock_yuman` se joint via\n`product_code` = `reference`). Les prix achat/vente sont les prix\ncourants Yuman, sans historique \u2014 toute valorisation est indicative.\n"""
    )
    as (
      

select
    product_id,
    product_code,
    product_name,
    product_type,
    product_brand,
    product_unit,
    product_purchase_price,
    product_sale_price,
    is_active,
    created_at,
    updated_at

from `evs-datastack-prod`.`prod_staging`.`stg_yuman__products`
    );
  