
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_techinique_tasks`
      
    
    

    
    OPTIONS(
      description="""Table interm\u00e9diaire des inter technique. Filtr\u00e9e sur les t\u00e2ches de type INTER TECHNIQUE (idtask_type=131)  & filtr\u00e9 sur label C4 D\u00e9tartrage/filtre avec statut actif (code_status_record=1) et enrichie avec code produit.\n"""
    )
    as (
      

with inter_base as (

    select
        -- Identifiants
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,

        -- Codes
        c.code as company_code,
        d.code as device_code,
        p.code as product_code,
        ts.code as task_status_code,
        la.code as label_code,
        lf.code as label_family_code,

        -- Informations métier
        t.real_start_date as task_start_date,
        t.real_end_date as task_end_date,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` t
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` thp on thp.idtask = t.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` c on c.idcompany = t.idcompany_peer
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` d on d.iddevice = t.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` p on p.idproduct = thp.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task` lht on t.idtask = lht.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` la on lht.idlabel = la.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` lf on la.idlabel_family = lf.idlabel_family
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` ts on t.idtask_status = ts.idtask_status

    where 1=1
        and t.idtask_status in (1, 4, 3)  -- FAIT, VALIDE, ANNULE
        and t.code_status_record = '1'
        and t.idtask_type = 131 -- INTERVENTION TECHNIQUE
        and t.real_start_date is not null
)

select
    -- Identifiants
    task_id,
    device_id,
    company_id,
    product_id,

    -- Codes
    company_code,
    device_code,
    product_code,
    task_status_code,

    -- Pivot des labels
    max(case when label_family_code = 'Statut inter' then label_code end) as statut_inter,
    max(case when label_family_code = 'DC04' then label_code end) as dc04,
    max(case when label_family_code = 'MISSION_TYPE' then label_code end) as mission_type,
    max(case when label_family_code = 'Objet intervent' then label_code end) as objet_intervent,

    -- Infos métier
    task_start_date,
    task_end_date,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from inter_base
group by
    task_id, device_id, company_id, product_id,
    company_code, device_code, product_code, task_status_code,
    task_start_date, task_end_date, updated_at, created_at, extracted_at
HAVING MAX(CASE WHEN label_family_code = 'Objet intervent' THEN label_code END) = 'OC04'
    );
  