

with device as (
    select
        iddevice,
        device_iddevice,
        iddevice_type,
        idmodel,
        idcompany_customer,
        idcompany_owner,
        code,
        name,
        serial,
        purchase_date,
        purchase_cost,
        last_installation_date,
        created_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__device`
    where coalesce(code_status_record, 0) != -1
),

label as (
    select
        iddevice,
        coalesce(lower(max(case when lf_code = 'ISACTIVE' then l_code end)) = 'yes', false) as is_active,
        coalesce(upper(max(case when lf_code = 'EQUIPEMENT_RESERVE' then l_code end)) = 'OUI', false) as is_reserved,
        max(case when lf_code = 'CATEGORIE' then l_code end) as device_category,
        max(case when lf_code = 'MARQUE_FOURNISSEUR' then l_code end) as device_brand,
        max(case when lf_code = 'GAMME' then l_code end) as device_range,
        max(case when lf_code = 'ETAT_MACHINE' then l_code end) as device_state,
        max(case when lf_code = 'STATUT_MATERIEL' then l_code end) as device_material_status
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_device`
    group by iddevice
),

modele as (
    select
        idproduct,
        code,
        name,
        purchase_unit_price
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product`
),

societe as (
    select
        idcompany,
        idcompany_type,
        code,
        name
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__company`
)

select
    d.iddevice as device_id,
    d.code as device_code,
    d.name as device_name,
    nullif(trim(d.serial), '') as serial_number,
    d.iddevice_type as device_type_id,
    d.device_iddevice as parent_device_id,
    d.idmodel as model_id,
    m.code as model_code,
    m.name as model_name,
    m.purchase_unit_price as model_purchase_unit_price,
    d.idcompany_customer as company_id,
    c.code as company_code,
    c.name as company_name,
    c.idcompany_type as company_type_id,
    d.idcompany_owner as owner_company_id,
    o.name as owner_company_name,
    o.idcompany_type as owner_company_type_id,
    coalesce(l.is_active, false) as is_active,
    coalesce(l.is_reserved, false) as is_reserved,
    l.device_category,
    l.device_brand,
    l.device_range,
    l.device_state,
    l.device_material_status,
    d.purchase_date,
    d.purchase_cost,
    d.last_installation_date,
    d.created_at
from device as d
left join label as l
    on d.iddevice = l.iddevice
left join modele as m
    on d.idmodel = m.idproduct
left join societe as c
    on d.idcompany_customer = c.idcompany
left join societe as o
    on d.idcompany_owner = o.idcompany