

with device_labels as (
    select
        d.iddevice as device_id,
        d.device_iddevice,
        d.iddevice_type as device_type_id,
        d.code as device_code,
        d.name as device_name,
        d.last_installation_date,
        d.created_at,
        d.updated_at,
        d.idlocation as location_id,
        d.idcompany_customer as company_id,
        c.code as company_code,
        c.name as company_name,
        lo.access_info,
        ld.label_code,
        ld.label_text_fr,
        ld.label_family_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_device` as ld
        on d.iddevice = ld.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c
        on d.idcompany_customer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` as lo
        on d.idlocation = lo.idlocation
    where d.idcompany_customer is not null
),

-- Roadman AFFECTÉ à la machine — état COURANT, source ERP contact_has_device.
-- Le "contact" est ici du personnel interne (approvisionneur / technicien) et non
-- un contact client : son `code` est aligné sur resources.code, et c'est le SEUL
-- lien disponible — la source ne porte aucune FK idresources.
-- Jointure sur dim_lcdp__resource (et non sur le staging) pour garantir que la FK
-- exposée résout toujours dans la dimension à laquelle la BI se relie.
-- DÉPARTAGE : 2 machines portent aujourd'hui 2 affectations (M7077, M5114) ; on
-- retient le code le plus petit, ce qui garantit 1 ligne par machine et une valeur
-- stable d'un rebuild à l'autre (le contact.ismain vaut 0 partout, inexploitable).
-- Le test assert_lcdp__device_single_assigned_roadman alerte si ces cas se multiplient.
-- ATTENTION : aucune historisation possible, la source n'a ni date de création ni
-- date de modification. C'est donc l'affectation d'aujourd'hui, pas celle du passé.
assigned_roadman as (
    select
        chd.iddevice as device_id,
        r.resources_id as assigned_roadman_id,
        r.resources_code as assigned_roadman_code,
        r.resources_name as assigned_roadman_name
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact_has_device` as chd
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact` as ct
        on chd.idcontact = ct.idcontact
    inner join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource` as r
        on ct.code = r.resources_code and r.resources_type = 'PERSON'
    qualify
        row_number() over (
            partition by chd.iddevice
            order by r.resources_code asc
        ) = 1
),

aggregated_labels as (
    select
        device_id,
        device_type_id,
        device_iddevice,
        company_id,
        location_id,
        device_code,
        device_name,
        company_code,
        company_name,
        access_info,
        last_installation_date,
        created_at,
        updated_at,
        max(case when label_family_code = 'CATMACH' then label_text_fr end) as device_category,
        max(case when label_family_code = 'STATUT_MATERIEL' then label_text_fr end) as device_material_status,
        max(case when label_family_code = 'TYPEAUDIT' then label_text_fr end) as audit_type,
        max(case when label_family_code = 'TYPFONT' then label_text_fr end) as fountain_type,
        max(case when label_family_code = 'TYPSP' then label_text_fr end) as type_sp,
        max(case when label_family_code = 'TYPBROY' then label_text_fr end) as grinder_type,
        max(case when label_family_code = 'ETAT_MACHINE' then label_text_fr end) as device_state,
        max(case when label_family_code = 'TYDA' then label_text_fr end) as typology_da,
        max(case when label_family_code = 'BADGE' then label_text_fr end) as badge,
        max(case when label_family_code = 'MARQUE' then label_text_fr end) as device_brand,
        max(case when label_family_code = 'MODSP' then label_text_fr end) as model_sp,
        -- is_active conservé sur le code (YES/NO) pour le test lower(...) = 'yes' ci-dessous
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        max(case when label_family_code = 'TYPDASA' then label_text_fr end) as type_dasa,
        max(case when label_family_code = 'MARQSP' then label_text_fr end) as brand_sp,
        max(case when label_family_code = 'TYPPERCO' then label_text_fr end) as percolator_type,
        max(case when label_family_code = 'LCDPMON' then label_text_fr end) as currency_mode
    from device_labels
    group by
        device_id,
        device_type_id,
        device_iddevice,
        company_id,
        location_id,
        device_code,
        device_name,
        company_code,
        company_name,
        access_info,
        last_installation_date,
        created_at,
        updated_at
)

select
    -- Identifiants
    al.device_id,
    al.device_iddevice,
    al.device_type_id,
    al.company_id,
    al.location_id,

    -- Codes et noms
    al.device_code,
    al.device_name,
    al.company_code,
    al.company_name,

    -- Caractéristiques machine
    al.device_category,
    al.device_brand,
    al.device_state,
    al.device_material_status,
    al.audit_type,
    al.typology_da,
    al.currency_mode,

    -- Types machine
    al.fountain_type,
    al.grinder_type,
    al.percolator_type,
    al.type_sp,
    al.type_dasa,
    al.model_sp,
    al.brand_sp,
    al.badge,

    -- Localisation
    al.access_info as device_location,

    -- Roadman AFFECTÉ (état courant ERP, cf. CTE assigned_roadman).
    -- À ne pas confondre avec le roadman OBSERVÉ de fct_lcdp__chargement_sortie,
    -- qui est celui ayant réellement chargé la machine sur la semaine.
    -- NULL pour ~73 % du parc (affectation renseignée sur 730 machines / 2 758),
    -- mais renseignée sur 99,4 % du périmètre DA FROID Nayax.
    ar.assigned_roadman_id,
    ar.assigned_roadman_code,
    ar.assigned_roadman_name,

    -- Statut
    coalesce(lower(al.is_active) = 'yes', false) as is_active,

    -- Dates
    al.last_installation_date,
    al.created_at,
    al.updated_at

from aggregated_labels as al
left join assigned_roadman as ar
    on al.device_id = ar.device_id