

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