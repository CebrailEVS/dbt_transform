
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__machines_yuman_maintenance_base`
      
    
    

    
    OPTIONS(
      description="""Table interm\u00e9diaire reliant les machines Oracle NESHU (issues de dim_oracle_neshu__device) aux mat\u00e9riels Yuman (materials, sites, clients, cat\u00e9gories).  Sert de dimension de r\u00e9f\u00e9rence pour les analyses de maintenance, enrichie avec informations client et site.\n"""
    )
    as (
      -- int_oracle_neshu__machines_yuman_maintenance_base.sql



-- LISTE MACHINE DLOG filtré & clean
WITH liste_machine_oracle AS (
    SELECT
      d.device_id,
      CONCAT('NESH_', d.device_code) AS device_code,
      d.device_name,
      CONCAT('NESH_', company_code) AS company_code,
      d.last_installation_date
    FROM `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` d
    WHERE is_active
      AND device_type_id IN (1, 2)
      AND REGEXP_CONTAINS(company_code, r'^CN[0-9]{4}$')
      AND device_name in ('MOMENTO 100', 'GEMINI 200', 'MOMENTO 200', 'MINITOWER GEMINI', 'MINITOWER MOMENTO',
        'SBS MOMENTO 100', 'TOWER GEMINI', 'TOWER MOMENTO', 'MILANO LYO FTS120',
        'MILANO GRAIN FTS60E', 'MILANO GRAIN FTS60E + MODULO', 'BLUSODA', 'BLUSODA GAZ',
        'TOWER BLUSODA', 'TOWER BLUSODA GAZ', 'MILANO LYO FTS120 + MODULO',
        'BLUSODA', 'TOWER ONE GAZ', 'OPTIBEANX12 + MODULO', 'OPTIBEAN X 12', 'OPTIBEAN X 12 TS','OPTIBEANX12TS + MODULO','OPTIBEANX12TS + MODULO','OPTIBEAN 12')
),
-- LISTE MACHINE YUMAN ENRICHI CLIENT / SITE filtré & clean
yuman_materials_clean  AS (
    SELECT 
        ym.material_id,
        ym.material_description,
        ym.material_name,
        ym.material_brand,
        ym.material_serial_number,
        ycat.category_name,
        ym.material_in_service_date,
        ym.created_at,
        ym.updated_at,
        yc.client_id,
        yc.client_code,
        yc.client_name,
        yc.client_category,
        yc.partner_name,
        ys.site_id,
        ys.site_code,
        ys.site_postal_code
    FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials` ym
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites` ys 
        ON ym.site_id = ys.site_id
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_yuman__clients` yc 
        ON ys.client_id = yc.client_id
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories` ycat 
        ON ycat.category_id = ym.category_id
    WHERE partner_name = 'NESHU'
      AND site_code NOT IN (
            "NESH_DEPOTATELIERBORDEAUX", "NESH_DEPOTATELIERLYON", "NESH_DEPOTATELIERMARSEILLE",
            "NESH_DEPOTATELIERRUNGIS", "NESH_DEPOTATELIERSTRASBOURG", "NESH_DEPOTBORDEAUX",
            "NESH_DEPOTLYON", "NESH_DEPOTMARSEILLE", "NESH_DEPOTPERIMES", "NESH_DEPOTREBUS",
            "NESH_DEPOTRUNGIS", "NESH_DEPOTSTRASBOURG", "NESH_RELYON", "NESH_RERUNGIS", "NESH_STOCKNUNSHEN"
        )
      AND material_name NOT LIKE '%GENERIQUE NESHU%'
      AND material_serial_number not in 
      ('NESH_MA00226', 'NESH_MA00227', 'NESH_MA00247', 'NESH_MA00248', 'NESH_MA00249',
      'NESH_MA00250', 'NESH_MA00251', 'NESH_MA00252', 'NESH_MA00253', 'NESH_MA00254',
      'NESH_MA00193', 'NESH_MA00194', 'NESH_MA00195', 'NESH_MA00196', 'NESH_MA00197',
      'NESH_MA00198', 'NESH_MA00199', 'NESH_MA00200', 'NESH_MA00201', 'NESH_MA00202',
      'NESH_MA00203', 'NESH_MA00204', 'NESH_MA00205', 'NESH_MA00206', 'NESH_MA00207',
      'NESH_MA00208', 'NESH_MA00209', 'NESH_MA00210', 'NESH_MA00211', 'NESH_MA00212',
      'NESH_MA00213', 'NESH_MA00214', 'NESH_MA00215', 'NESH_MA00228', 'NESH_MA00229',
      'NESH_MA00230', 'NESH_MA00231', 'NESH_MA00232', 'NESH_MA00233', 'NESH_MA00234',
      'NESH_MA00235', 'NESH_MA00236', 'NESH_MA00237', 'NESH_MA00238', 'NESH_MA00239',
      'NESH_MA00240', 'NESH_MA00241', 'NESH_MA00242', 'NESH_MA00244', 'NESH_MA00245',
      'NESH_MA00246', 'NESH_MA00256', 'NESH_MA00257', 'NESH_MA00258', 'NESH_MA00259',
      'NESH_MA00260', 'NESH_MA00261', 'NESH_MA00262', 'NESH_MA00263', 'NESH_MA00264',
      'NESH_MA00265', 'NESH_MA00266', 'NESH_MA00270', 'NESH_MA00216', 'NESH_MA00217',
      'NESH_MA00220', 'NESH_MA00221', 'NESH_MA00222', 'NESH_MA00223', 'NESH_MA00181',
      'NESH_MA00182', 'NESH_MA00184', 'NESH_MA00185', 'NESH_MA00183', 'NESH_MA00186',
      'NESH_MA00187', 'NESH_AS00401', 'NESH_AS00403', 'NESH_AS00393', 'NESH_AS00557',
      'NESH_AS00558', 'NESH_AS00242', 'NESH_AS00241', 'NESH_AS00562', 'NESH_AS00559',
      'NESH_AS00070', 'NESH_AS00568', 'NESH_AS00563', 'NESH_AS00561', 'NESH_AS00560',
      'NESH_AS00317', 'NESH_AS00314', 'NESH_MA00136', 'NESH_AS00011', 'NESH_AS00012',
      'NESH_MA00170', 'NESH_AS00004')
),
-- JOINTURE ENTRE LA LISTE MACHINE DLOG avec les données YUMAN
merged_materials_dlog_yuman AS (
    SELECT 
        lo.device_id,
        ym.material_id,
        lo.device_code,
        lo.device_name,
        lo.company_code,
        lo.last_installation_date,
        ym.material_serial_number,
        ym.client_id,
        ym.client_code,
        ym.client_name,
        ym.client_category,
        ym.site_id,
        ym.site_code,
        ym.site_postal_code,
        ym.created_at AS material_created_at,
        ym.updated_at AS material_updated_at
    FROM liste_machine_oracle lo
    LEFT JOIN yuman_materials_clean ym
        ON lo.device_code = ym.material_serial_number
)
SELECT * from merged_materials_dlog_yuman
    );
  