
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_products`
      
    
    

    
    OPTIONS(
      description="""Produits utilis\u00e9s lors des interventions (workorders). 1 ligne = 1 produit utilis\u00e9 dans une intervention. Cette table d\u00e9plie le JSON _embed_products de la table yuman_workorders."""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_workorders`

),

workorder_products_unnested as (

    select
        wo.id as workorder_id,
        cast(json_extract_scalar(product, '$.id') as int64) as workorder_product_id,
        cast(json_extract_scalar(product, '$.product_id') as int64) as product_id,
        json_extract_scalar(product, '$.product_reference') as product_reference,
        json_extract_scalar(product, '$.product_designation') as product_designation,
        cast(json_extract_scalar(product, '$.quantity') as float64) as product_quantity,
        timestamp(json_extract_scalar(product, '$.created_at')) as product_created_at,
        timestamp(json_extract_scalar(product, '$.updated_at')) as product_updated_at
    from source_data as wo
    cross join unnest(json_extract_array(wo._embed_products)) as product

),

final as (

    select
        workorder_product_id,
        workorder_id,
        product_id,
        product_reference,
        product_designation,
        product_quantity,
        product_created_at,
        product_updated_at
    from workorder_products_unnested

)

select *
from final
    );
  