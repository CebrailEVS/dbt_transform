

-- Référentiel des machines Neshu pour Cockpit Supply : préventives TechCare et écran Équipements en stock.
-- Construit depuis le staging, sans passer par dim_neshu__device : l'app et la BI évoluent indépendamment.

with labels as (
    select
        lhd.iddevice,
        max(case when lf.code = 'ISACTIVE' then l.code end) as is_active,
        max(case when lf.code = 'EQUIPEMENT_RESERVE' then l.code end) as is_reserved,
        max(case when lf.code = 'ETAT_MACHINE' then l.code end) as device_state,
        max(case when lf.code = 'STATUT_MATERIEL' then l.code end) as device_material_status,
        max(case when lf.code = 'MARQUE' then l.code end) as device_brand,
        max(case when lf.code = 'GAMME' then l.code end) as device_gamme,
        max(case when lf.code = 'CATEGORIE' then l.code end) as device_category
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_device` as lhd
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhd.idlabel = l.idlabel
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    group by lhd.iddevice
)

select
    d.iddevice as device_id,
    d.code as device_code,
    d.name as device_name,
    d.serial as device_serial_number,
    c.code as company_code,
    c.name as company_name,
    o.name as owner_company_name,
    lb.device_brand,
    lb.device_gamme,
    lb.device_category,
    lb.device_state,
    lb.device_material_status,
    d.comments as device_comment,
    coalesce(lower(lb.is_active) = 'yes', false) as is_active,
    -- Même famille et même code que l'ERP Nayka ; FALSE tant que la famille n'existe pas dans l'ERP Neshu.
    coalesce(upper(lb.is_reserved) = 'OUI', false) as is_reserved,
    d.purchase_date,
    d.purchase_cost
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
left join labels as lb
    on d.iddevice = lb.iddevice
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
    on d.idcompany_customer = c.idcompany
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as o
    on d.idcompany_owner = o.idcompany
where
    d.idcompany_customer is not null
    and d.code_status_record <> -1  -- ghost-deletes ERP