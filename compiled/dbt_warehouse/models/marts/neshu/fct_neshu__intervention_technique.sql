

with interventions as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_technique_tasks`
),

devices as (
    select
        device_id,
        device_name
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
),

companies as (
    select
        company_id,
        company_name
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
),

resources as (
    select
        resources_id,
        resources_code,
        gea_code,
        company_storehouse_id
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__resource`
)

select
    -- Grain
    date(i.task_start_date, 'Europe/Paris') as intervention_date,
    i.task_id,

    -- FK
    i.device_id,
    i.modele_equipement_id,
    i.company_id,
    i.resources_id,
    i.product_id as piece_id,

    -- Attributs d'affichage
    i.document_number,
    i.device_code,
    d.device_name,
    i.company_code,
    c.company_name,
    r.resources_code as roadman_code,
    r.gea_code,
    -- Agence = dépôt de rattachement actuel de l'intervenant, pas celui du client.
    a.company_name as agence,
    i.product_code as piece_code,

    -- Statut et labels : code + libellé fr_FR
    i.task_status_code,
    i.statut_inter,
    i.statut_inter_label,
    i.objet_intervent,
    i.objet_intervent_label,
    i.objet_intervent_detail,
    i.objet_intervent_detail_label,
    i.affichage,
    i.affichage_label,
    i.facturation,
    i.facturation_label,

    -- Commentaires
    i.comments_peer,
    i.comments_self,

    -- Dates (heure de Paris)
    timestamp(datetime(i.task_start_date, 'Europe/Paris')) as task_start_date,
    timestamp(datetime(i.task_end_date, 'Europe/Paris')) as task_end_date,

    -- Métadonnées (heure de Paris)
    timestamp(datetime(i.created_at, 'Europe/Paris')) as created_at,
    timestamp(datetime(i.updated_at, 'Europe/Paris')) as updated_at

from interventions as i
left join devices as d
    on i.device_id = d.device_id
left join companies as c
    on i.company_id = c.company_id
left join resources as r
    on i.resources_id = r.resources_id
left join companies as a
    on r.company_storehouse_id = a.company_id