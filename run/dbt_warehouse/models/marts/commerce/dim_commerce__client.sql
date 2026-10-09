
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_commerce__client`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nDimension client commerciale Nespresso : r\u00e9f\u00e9rentiel des tiers (clients\net prospects) avec leurs attributs descriptifs (adresse, donneur d'ordre,\nsegmentation, m\u00e9tier, r\u00e9gion, SIRET, adh\u00e9sion club) enrichi de l'ID C4C\nle plus r\u00e9cent observ\u00e9 pour ce tiers.\n\n[COMMENT CONSTRUITE]\nConstruit \u00e0 partir de la base clients Nespresso (`stg_nesp_co__client`,\nissu du fichier Excel `nespresso_base_client`). Le tiers (`third`) est\nla cl\u00e9 m\u00e9tier Nessoft. L'identifiant C4C (`third_c4c_id`) est d\u00e9riv\u00e9\npar d\u00e9duplication temporelle : pour chaque tiers, on cherche dans les\nopportunit\u00e9s (`stg_nesp_co__opportunite`) et les activit\u00e9s\n(`stg_nesp_co__activite`) l'ID C4C le plus r\u00e9cent (max sur la date de\ncr\u00e9ation de l'opportunit\u00e9 ou de d\u00e9but de l'activit\u00e9).\n\n[GRAIN]\n1 ligne par `third`.\n\n[NOTES]\nSource : fichier Excel nesp_co. Noms de colonnes en fran\u00e7ais pour\npr\u00e9server la compatibilit\u00e9 avec les rapports Power BI. \u00c9cart assum\u00e9\navec les conventions marts (noms en anglais).\n"""
    )
    as (
      

select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__clients_enrichis`
    );
  