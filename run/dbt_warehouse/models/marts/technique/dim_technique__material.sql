
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_technique__material`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nDimension mat\u00e9riel (machine) Yuman \u2014 chaque machine d\u00e9ploy\u00e9e chez un\nclient EVS, enrichie avec sa cat\u00e9gorie technique (machine \u00e0 caf\u00e9, fontaine,\netc.).\n\n[COMMENT CONSTRUITE]\nLecture de `stg_yuman__materials` joint \u00e0 `stg_yuman__materials_categories`\nsur `category_id` pour ajouter `category_name`.\n\n[GRAIN]\n1 ligne par `material_id` (PK Yuman).\n\n[NOTES]\n`site_id` aplati directement pour faciliter les jointures BI (FK vers\n`dim_technique__site`).\n"""
    )
    as (
      

select
    ym.material_id,
    ym.site_id,
    ym.material_description,
    ym.material_name,
    ym.material_brand,
    ym.material_serial_number,
    ycat.category_name,
    ym.material_in_service_date,
    ym.is_active as material_is_active,
    ym.created_at,
    ym.updated_at

from `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials` as ym
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories` as ycat
    on ym.category_id = ycat.category_id
    );
  