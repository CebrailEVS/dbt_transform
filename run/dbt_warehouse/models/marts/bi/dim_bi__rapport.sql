
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension du parc de rapports Power BI r\u00e9ellement exploit\u00e9s : les rapports m\u00e9tier des espaces de travail partag\u00e9s, hors artefacts techniques g\u00e9n\u00e9r\u00e9s par la plateforme.\n[COMMENT CONSTRUITE] Jointure interne de stg_powerbi_activity__reports (d\u00e9j\u00e0 purg\u00e9 des copies d'App et des rapports de m\u00e9triques d'usage) sur stg_powerbi_activity__workspaces (d\u00e9j\u00e0 restreint aux espaces partag\u00e9s actifs). C'est cette jointure interne qui mat\u00e9rialise le troisi\u00e8me filtre obligatoire du contrat de source et fait passer de 100 rapports \u00e0 37. Aplatit `workspace_name` de l'espace parent, `dataset_name` et `is_refreshable` du mod\u00e8le s\u00e9mantique parent.\n[GRAIN] 1 ligne par report_id (PK). 37 lignes au 2026-09-03.\n[NOTES] Sans les trois filtres, le parc afficherait 139 rapports dont 55 \u00ab jamais consult\u00e9s \u00bb \u2014 chiffre faux dans le sens alarmiste, et d\u00e9j\u00e0 publi\u00e9 une fois par erreur. Le d\u00e9compte de r\u00e9f\u00e9rence est : 139 bruts \u2192 76 en espace partag\u00e9 \u2192 47 hors copies d'App \u2192 37 hors m\u00e9triques d'usage. `dataset_id` est test\u00e9 `unique` : le parc est en 1:1 rapport/mod\u00e8le s\u00e9mantique, et les deux faits de cette BU s'appuient sur cette propri\u00e9t\u00e9 pour rattacher les rafra\u00eechissements au rapport.\n"""
    )
    as (
      

with rapports as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__reports`
),

espaces as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__workspaces`
),

modeles as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__datasets`
)

select
    -- grain : 1 ligne = 1 rapport du parc metier
    r.report_id,

    -- cles etrangeres
    r.workspace_id,
    r.dataset_id,

    -- attributs
    r.report_name,
    -- attributs d'affichage aplatis des parents directs (1 colonne pour
    -- l'espace, 2 pour le modele) : evite une jointure cote Power BI sans
    -- aplatir la dim parente entiere
    e.workspace_name,
    m.dataset_name,
    r.format,
    r.created_by,
    r.modified_by,
    r.web_url,

    -- booleens
    m.is_refreshable,

    -- metadonnees
    r.created_at,
    r.updated_at,
    r.extracted_at

from rapports as r
-- La jointure INTERNE sur les espaces de travail EST le troisieme filtre
-- obligatoire du contrat de source : elle restreint le parc aux seuls espaces
-- partages et actifs. C'est ce qui fait passer de 100 rapports (staging) aux
-- 37 rapports metier. Ne jamais la passer en LEFT.
inner join espaces as e on r.workspace_id = e.workspace_id
left join modeles as m on r.dataset_id = m.dataset_id
    );
  