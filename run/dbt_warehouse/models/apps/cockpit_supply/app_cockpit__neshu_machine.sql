
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des machines Neshu (actives ou non), tel que lu par l'application Cockpit Supply : pr\u00e9ventives TechCare et \u00e9cran \u00c9quipements en stock (machines en d\u00e9p\u00f4t ou en atelier, avec n\u00b0 de s\u00e9rie, propri\u00e9taire, achat et r\u00e9servation).\n[COMMENT CONSTRUITE] Construit depuis le staging, ind\u00e9pendamment de dim_neshu__device : stg_oracle_neshu__device (hors machines sans soci\u00e9t\u00e9 de rattachement et hors ghost-deletes ERP), pivot des labels (stg_oracle_neshu__label_has_device, label, label_family) et stg_oracle_neshu__company pour le d\u00e9p\u00f4t ou client et pour le propri\u00e9taire. Colonnes limit\u00e9es \u00e0 celles des deux \u00e9crans. Pas de filtre de d\u00e9p\u00f4t ni de statut.\n[GRAIN] 1 ligne par device_id.\n[NOTES] lue via get_dim_devices_neshu ; \u00e9crans : technique_terrain, \u00e9quipements en stock. Le d\u00e9p\u00f4t d'une machine en stock est company_code / company_name (ex. DEPOTATELIERRUNGIS / \u00ab 06 - ATELIER RUNGIS DEPOT \u00bb) ; company_code se rapproche de destination_code de app_cockpit__neshu_commande_fournisseur. Les labels sont des codes ERP (device_material_status S = en stock), contrairement \u00e0 LCDP qui expose des libell\u00e9s. Les r\u00e8gles de l'app (d\u00e9p\u00f4ts retenus, statuts, exclusions) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

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
    );
  