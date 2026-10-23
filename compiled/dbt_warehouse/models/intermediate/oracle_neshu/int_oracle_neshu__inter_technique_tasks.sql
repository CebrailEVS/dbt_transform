

with tasks as (

    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task`
    where
        idtask_type = 131 -- INTERVENTION TECHNIQUE
        and idtask_status in (1, 4, 3, 5) -- FAIT, VALIDE, ANNULE, ANOMALIE
        and code_status_record = '1'
        and real_start_date is not null

),

-- Intervenant : la ressource PERSON de la tâche (roadman ou GEA), une seule par tâche.
intervenant as (

    select
        thr.idtask as task_id,
        thr.idresources as resources_id

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` as thr
    inner join tasks as t
        on thr.idtask = t.idtask
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
        on thr.idresources = r.idresources

    where r.idresources_type = 2 -- PERSON

),

labels_fr as (

    select
        idstring,
        trim(text) as label_text
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__string`
    where langage_code = 'fr_FR'

),

task_labels as (

    select
        lht.idtask as task_id,
        lf.code as family_code,
        la.code as label_code,
        fr.label_text

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task` as lht
    inner join tasks as t
        on lht.idtask = t.idtask
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as la
        on lht.idlabel = la.idlabel
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on la.idlabel_family = lf.idlabel_family
    left join labels_fr as fr
        on la.idstring = fr.idstring

),

-- Chaque objet OCxx a sa famille de détail DCxx. Une tâche peut porter deux objets : on retient
-- celui qui a un détail, puis le plus petit code, pour que l'objet et son détail restent cohérents.
objet as (

    select
        o.task_id,
        o.label_code as objet_intervent,
        o.label_text as objet_intervent_label,
        d.label_code as objet_intervent_detail,
        d.label_text as objet_intervent_detail_label

    from task_labels as o
    left join task_labels as d
        on
            o.task_id = d.task_id
            and d.family_code = concat('DC', right(o.label_code, 2))

    where o.family_code = 'Objet intervent'

    qualify row_number() over (
        partition by o.task_id
        order by d.label_code is null, o.label_code
    ) = 1

),

-- Deux statuts sur une même tâche (TERMINATED + TOTECH) : TERMINATED l'emporte, comme chez LCDP.
label_pivot as (

    select
        task_id,
        coalesce(
            max(case when family_code = 'Statut inter' and label_code = 'TERMINATED' then label_code end),
            max(case when family_code = 'Statut inter' then label_code end)
        ) as statut_inter,
        coalesce(
            max(case when family_code = 'Statut inter' and label_code = 'TERMINATED' then label_text end),
            max(case when family_code = 'Statut inter' then label_text end)
        ) as statut_inter_label,
        max(case when family_code = 'MISSION_TYPE' then label_code end) as mission_type,
        max(case when family_code = 'MISSION_TYPE' then label_text end) as mission_type_label,
        max(case when family_code = 'AFFICHAGE' then label_code end) as affichage,
        max(case when family_code = 'AFFICHAGE' then label_text end) as affichage_label,
        max(case when family_code = 'FACTURATION' then label_code end) as facturation,
        max(case when family_code = 'FACTURATION' then label_text end) as facturation_label,
        max(case when family_code = 'DEVICE_CANCEL_REASON' then label_code end) as device_cancel_reason,
        max(case when family_code = 'DEVICE_CANCEL_REASON' then label_text end) as device_cancel_reason_label,
        max(case when family_code = 'INTEGRATION_TYPE' then label_code end) as integration_type,
        max(case when family_code = 'INTEGRATION_TYPE' then label_text end) as integration_type_label

    from task_labels
    group by task_id

)

select
    -- Identifiants
    t.idtask as task_id,
    t.iddevice as device_id,
    d.idmodel as modele_equipement_id,
    t.idcompany_peer as company_id,
    thp.idproduct as product_id,
    i.resources_id,

    -- Codes
    t.document_number,
    c.code as company_code,
    d.code as device_code,
    p.code as product_code,
    ts.code as task_status_code,

    -- Labels : code + libellé fr_FR
    lp.statut_inter,
    lp.statut_inter_label,
    o.objet_intervent,
    o.objet_intervent_label,
    o.objet_intervent_detail,
    o.objet_intervent_detail_label,
    lp.mission_type,
    lp.mission_type_label,
    lp.affichage,
    lp.affichage_label,
    lp.facturation,
    lp.facturation_label,
    lp.device_cancel_reason,
    lp.device_cancel_reason_label,
    lp.integration_type,
    lp.integration_type_label,

    -- Infos métier
    t.comments_self,
    t.comments_peer,
    t.real_start_date as task_start_date,
    t.real_end_date as task_end_date,

    -- Timestamps techniques
    t.updated_at,
    t.created_at,
    t.extracted_at

from tasks as t
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
    on t.idtask = thp.idtask
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p
    on thp.idproduct = p.idproduct
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
    on t.idcompany_peer = c.idcompany
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
    on t.iddevice = d.iddevice
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` as ts
    on t.idtask_status = ts.idtask_status
left join intervenant as i
    on t.idtask = i.task_id
left join objet as o
    on t.idtask = o.task_id
left join label_pivot as lp
    on t.idtask = lp.task_id