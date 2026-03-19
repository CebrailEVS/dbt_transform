
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__resources`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with resources_labels as (
    select
        r.idresources as resources_id,
        r.idresources_type as resources_type_id,
        r.resources_idresources,
        r.idcompany as company_id,
        r.idcompany_storehouse as company_storehouse_id,
        r.idlocation as location_id,
        r.code as resources_code,
        r.name as resources_name,
        r.cost,
        r.arrival,
        r.departure,
        r.code_status_record,
        r.created_at,
        r.updated_at,
        rt.code as resources_type,
        g.gea_code,
        l.code as label_code,
        lf.code as label_family_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources_type` as rt
        on r.idresources_type = rt.idresources_type
    left join `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__roadman_gea` as g
        on r.code = g.roadman_code and rt.code = 'PERSON'
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_resources` as lhr
        on r.idresources = lhr.idresources and lhr.idlabel is not null
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhr.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    where rt.code in ('PERSON', 'VEHICLE')
    --   and r.code_status_record = 1
),

aggregated_labels as (
    select
        resources_id,
        resources_type_id,
        resources_idresources,
        company_id,
        company_storehouse_id,
        location_id,
        resources_code,
        resources_name,
        resources_type,
        gea_code,
        cost,
        arrival,
        departure,
        code_status_record,
        created_at,
        updated_at,
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        max(case when label_family_code = 'Fonction' then label_code end) as fonction,
        max(case when label_family_code = 'DOTATION' then label_code end) as dotation,
        max(
            case when label_family_code = 'EXPORT_DATAMOBILE_USER' then label_code end
        ) as export_datamobile_user
    from resources_labels
    group by
        resources_id,
        resources_type_id,
        resources_idresources,
        company_id,
        company_storehouse_id,
        location_id,
        resources_code,
        resources_name,
        resources_type,
        gea_code,
        cost,
        arrival,
        departure,
        code_status_record,
        created_at,
        updated_at
)

select
    -- 🔑 Identifiants
    resources_id,
    resources_type_id,
    resources_idresources,
    company_id,
    company_storehouse_id,
    location_id,

    -- 📇 Codes et noms
    resources_code,
    resources_name,
    resources_type,
    gea_code,

    -- 🏷️ Caractéristiques
    coalesce(lower(is_active) = 'yes', false) as is_active,
    fonction,

    -- 💰 Opérationnel
    cost,
    arrival,
    departure,

    code_status_record,

    -- 🕒 Dates
    created_at,
    updated_at

from aggregated_labels
    );
  