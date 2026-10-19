
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_machine`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des machines Caf\u00e9s du Phare, tel que lu par l'application Cockpit Supply : comptage par approvisionneur et cat\u00e9gorie, et \u00e9cran \u00c9quipements en stock (machines en d\u00e9p\u00f4t, avec n\u00b0 de s\u00e9rie, propri\u00e9taire et achat).\n[COMMENT CONSTRUITE] Construit depuis le staging, ind\u00e9pendamment de dim_lcdp__device : stg_oracle_lcdp__device (hors machines sans soci\u00e9t\u00e9 de rattachement), libell\u00e9s des labels (stg_oracle_lcdp__label_device), stg_oracle_lcdp__company pour le d\u00e9p\u00f4t ou client et pour le propri\u00e9taire, roadman affect\u00e9 via stg_oracle_lcdp__contact_has_device \u2192 contact \u2192 resources (type PERSON, code le plus petit si deux affectations). Colonnes limit\u00e9es \u00e0 celles des deux \u00e9crans. Pas de filtre de d\u00e9p\u00f4t ni de statut.\n[GRAIN] 1 ligne par device_id.\n[NOTES] lue via get_dim_devices_lcdp ; \u00e9crans : lcdp_pilotage, \u00e9quipements en stock. Le d\u00e9p\u00f4t d'une machine en stock est company_code / company_name (DEPOT ATELIER, DEPOT NEUF, DEPOT CASSE, DEPOT BOUTIQUE). Les labels sont des libell\u00e9s (device_material_status \u00ab EN STOCK \u00bb), contrairement \u00e0 Neshu qui expose des codes. Pas de champ de r\u00e9servation : la famille n'existe pas dans l'ERP LCDP. Les r\u00e8gles de l'app (d\u00e9p\u00f4ts retenus, statuts, exclusions) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des machines Cafés du Phare pour Cockpit Supply : comptage par approvisionneur et catégorie, et
-- écran Équipements en stock. Construit depuis le staging, sans passer par dim_lcdp__device : l'app et la BI
-- évoluent indépendamment.

with labels as (
    -- Libellés FR, sauf ISACTIVE gardé sur le code (YES/NO) pour la conversion booléenne.
    select
        iddevice,
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        max(case when label_family_code = 'CATMACH' then label_text_fr end) as device_category,
        max(case when label_family_code = 'MARQUE' then label_text_fr end) as device_brand,
        max(case when label_family_code = 'ETAT_MACHINE' then label_text_fr end) as device_state,
        max(case when label_family_code = 'STATUT_MATERIEL' then label_text_fr end) as device_material_status
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_device`
    group by iddevice
),

-- Roadman affecté (état courant ERP) : le contact est du personnel interne dont le code est celui de la
-- ressource PERSON, seul lien disponible. Deux affectations possibles : le plus petit code, stable d'un build à l'autre.
assigned_roadman as (
    select
        chd.iddevice,
        r.code as assigned_roadman_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact_has_device` as chd
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact` as ct
        on chd.idcontact = ct.idcontact
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r
        on ct.code = r.code
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources_type` as rt
        on r.idresources_type = rt.idresources_type and rt.code = 'PERSON'
    qualify row_number() over (partition by chd.iddevice order by r.code asc) = 1
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
    lb.device_category,
    lb.device_state,
    lb.device_material_status,
    d.comments as device_comment,
    ar.assigned_roadman_code,
    coalesce(lower(lb.is_active) = 'yes', false) as is_active,
    d.purchase_date,
    d.purchase_cost
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d
left join labels as lb
    on d.iddevice = lb.iddevice
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c
    on d.idcompany_customer = c.idcompany
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as o
    on d.idcompany_owner = o.idcompany
left join assigned_roadman as ar
    on d.iddevice = ar.iddevice
where d.idcompany_customer is not null
    );
  