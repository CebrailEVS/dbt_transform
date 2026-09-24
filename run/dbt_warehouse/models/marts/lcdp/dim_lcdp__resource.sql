
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension ressource Oracle LCDP : personnes (PERSON) + v\u00e9hicules (VEHICLE) avec hi\u00e9rarchie.\n[COMMENT CONSTRUITE] Issu de stg_oracle_lcdp__resources, pivot des labels via stg_oracle_lcdp__label_has_resources : ISACTIVE, Fonction. Miroir de dim_neshu__resource sans l'enrichissement GEA (pas de seed c\u00f4t\u00e9 LCDP).\n[GRAIN] 1 ligne par resources_id.\n[NOTES] Hi\u00e9rarchie : resources_idresources d'un VEHICLE pointe vers l'ID de la PERSON associ\u00e9e ; NULL pour les personnes. code_status_record conserv\u00e9 (1=actif) sans filtre \u2014 filtrage \u00e0 la vol\u00e9e c\u00f4t\u00e9 BI. is_active converti en bool\u00e9en.\n"""
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
        l.code as label_code,
        lf.code as label_family_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources_type` as rt
        on r.idresources_type = rt.idresources_type
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_resources` as lhr
        on r.idresources = lhr.idresources and lhr.idlabel is not null
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as l
        on lhr.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    where rt.code in ('PERSON', 'VEHICLE')
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
        cost,
        arrival,
        departure,
        code_status_record,
        created_at,
        updated_at,
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        max(case when label_family_code = 'Fonction' then label_code end) as fonction
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
  