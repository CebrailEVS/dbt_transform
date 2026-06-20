
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_technique__parc_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nVue parc machine consolid\u00e9e \u2014 chaque machine d\u00e9ploy\u00e9e chez un client\nEVS avec ses attributs site et client aplatis. Consomm\u00e9e directement par\nun BI cartographie partenaires.\n\n[COMMENT CONSTRUITE]\nLecture de `stg_yuman__materials` joint \u00e0 `stg_yuman__sites` (sur site_id)\net `stg_yuman__clients` (sur client_id via site) + cat\u00e9gorie via\n`stg_yuman__materials_categories`. C'est une dim \"enrichie\" qui aplatit\nles attributs de 2 dims parentes (site + client).\n\n[GRAIN]\n1 ligne par `material_id` (PK).\n\n[NOTES]\nOBT-like : l'aplatissement multi-dim est volontaire car le BI parc\nmachine consomme directement cette table sans jointure c\u00f4t\u00e9 Power Query.\nSi 3+ rapports r\u00e9p\u00e8tent ce flatten, voir si on consolide ; sinon on garde.\nPattern doc dans `docs/conventions/marts.md`.\n"""
    )
    as (
      

select
    -- Informations Machine
    ym.material_id,
    ym.material_description,
    ym.material_name,
    ym.material_brand,
    ym.material_serial_number,
    ycat.category_name,
    ym.material_in_service_date,

    -- Informations Client
    yc.client_id,
    yc.client_code,
    yc.client_address,
    yc.client_name,
    yc.partner_name,

    -- Informations Site
    ys.site_id,
    ys.site_address,
    ys.site_name,
    ys.site_postal_code

from `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials` as ym
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites` as ys
    on ym.site_id = ys.site_id
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__clients` as yc
    on ys.client_id = yc.client_id
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories` as ycat
    on ym.category_id = ycat.category_id
    );
  