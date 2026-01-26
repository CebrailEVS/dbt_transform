
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valorisation_parc_machines`
      
    
    

    
    OPTIONS(
      description="""Table interm\u00e9diaire de valorisation du parc machines NESHU par device_name et device_group en filtrant sur les clients actifs NESHU et calculant la valorisation \u00e0 partir des r\u00e9f\u00e9rences de capacit\u00e9 et des prix d'achat des produits li\u00e9 \u00e0 chaque machine/groupe de machines.\n"""
    )
    as (
      

WITH 
-- Étape 0 : Filtrer les clients actifs NESHU (company_code = "CN" + 4 chiffres)
companies_filtered AS (
  SELECT 
    company_id,
    company_code
  FROM `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company`
  WHERE 
    is_active = TRUE
    AND company_type = "CLIENTNESHU"
    AND REGEXP_CONTAINS(company_code, r'^CN[0-9]{4}$')
),
-- Étape 1 : Récupérer les machines actives par client filtrées
active_devices AS (
  SELECT 
    d.device_id,
    d.company_id,
    cf.company_code,
    d.device_name
  FROM `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` d
  INNER JOIN companies_filtered cf
    ON d.company_id = cf.company_id
  WHERE 
    d.is_active = TRUE 
    AND d.device_iddevice IS NULL
    AND d.device_name IN (
      SELECT DISTINCT device_name 
      FROM `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__groupement_machine`
    )
),

-- Étape 2 : Mapper chaque device avec son device_group
devices_with_group AS (
  SELECT 
    ad.device_id,
    ad.company_id,
    ad.company_code,
    ad.device_name,
    gm.device_group
  FROM active_devices ad
  INNER JOIN `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__groupement_machine` gm
    ON ad.device_name = gm.device_name
),

-- Étape 3 : Joindre avec la table de valorisation
devices_with_products AS (
  SELECT 
    dwg.device_id,
    dwg.company_id,
    dwg.company_code,
    dwg.device_name,
    dwg.device_group,
    vm.product_code,
    vm.quantite
  FROM devices_with_group dwg
  LEFT JOIN `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__valo_machine_capacite` vm
    ON LOWER(dwg.device_group) = LOWER(vm.device_group)
),

-- Étape 4 : Joindre avec la table produit pour obtenir les prix
devices_with_prices AS (
  SELECT 
    dwp.device_id,
    dwp.company_id,
    dwp.company_code,
    dwp.device_name,
    dwp.device_group,
    dwp.product_code,
    dwp.quantite,
    p.purchase_unit_price,
    (dwp.quantite * p.purchase_unit_price) AS valorisation_produit
  FROM devices_with_products dwp
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` p
    ON dwp.product_code = p.product_code
),

-- Étape 5 : Calculer la valorisation totale par machine (device)
valorisation_by_device AS (
  SELECT 
    device_id,
    device_name,
    device_group,
    SUM(valorisation_produit) AS valorisation_totale_machine
  FROM devices_with_prices
  GROUP BY device_id, device_name, device_group
),

final_result AS (
-- Résultat final : regroupé par device_name / device_group
SELECT
  device_name,
  device_group,
  COUNT(device_id) AS nombre_machines,
  ROUND(SUM(valorisation_totale_machine), 2) AS valorisation_totale_machine
FROM valorisation_by_device
GROUP BY device_name, device_group
)

-- Ajout métadonnées dbt
SELECT 
    device_name,
    device_group,
    nombre_machines,
    valorisation_totale_machine,
    -- Métadonnées d'exécution
    CURRENT_TIMESTAMP() as dbt_updated_at,
    'df54b2db-e6d3-436a-9983-5d9f69a27b49' as dbt_invocation_id
FROM final_result
    );
  