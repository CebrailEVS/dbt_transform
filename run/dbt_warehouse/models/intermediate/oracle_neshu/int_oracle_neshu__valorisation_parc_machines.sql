
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valorisation_parc_machines`
      
    
    

    
    OPTIONS(
      description="""Table interm\u00e9diaire de valorisation du parc machines NESHU par device_name et device_group en filtrant sur les clients actifs NESHU et calculant la valorisation \u00e0 partir des r\u00e9f\u00e9rences de capacit\u00e9 et des prix d'achat des produits li\u00e9 \u00e0 chaque machine/groupe de machines.\n"""
    )
    as (
      

with
-- Étape 0 : Filtrer les clients actifs NESHU (company_code = "CN" + 4 chiffres)
companies_filtered as (
    select
        company_id,
        company_code
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company`
    where
        is_active = true
        and company_type = "CLIENTNESHU"
        and regexp_contains(company_code, r'^CN[0-9]{4}$')  -- noqa: CV10
),

-- Étape 1 : Récupérer les machines actives par client filtrées
active_devices as (
    select
        d.device_id,
        d.company_id,
        cf.company_code,
        d.device_name
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` as d
    inner join companies_filtered as cf
        on d.company_id = cf.company_id
    where
        d.is_active = true
        and d.device_iddevice is null
        and d.device_name in (
            select distinct device_name  -- noqa: RF02
            from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__groupement_machine`
        )
),

-- Étape 2 : Mapper chaque device avec son device_group
devices_with_group as (
    select
        ad.device_id,
        ad.company_id,
        ad.company_code,
        ad.device_name,
        gm.device_group
    from active_devices as ad
    inner join `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__groupement_machine` as gm
        on ad.device_name = gm.device_name
),

-- Étape 3 : Joindre avec la table de valorisation
devices_with_products as (
    select
        dwg.device_id,
        dwg.company_id,
        dwg.company_code,
        dwg.device_name,
        dwg.device_group,
        vm.product_code,
        vm.quantite
    from devices_with_group as dwg
    left join `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__valo_machine_capacite` as vm
        on lower(dwg.device_group) = lower(vm.device_group)
),

-- Étape 4 : Joindre avec la table produit pour obtenir les prix
devices_with_prices as (
    select
        dwp.device_id,
        dwp.company_id,
        dwp.company_code,
        dwp.device_name,
        dwp.device_group,
        dwp.product_code,
        dwp.quantite,
        p.purchase_unit_price,
        (dwp.quantite * p.purchase_unit_price) as valorisation_produit
    from devices_with_products as dwp
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p
        on dwp.product_code = p.product_code
),

-- Étape 5 : Calculer la valorisation totale par machine (device)
valorisation_by_device as (
    select
        device_id,
        device_name,
        device_group,
        sum(valorisation_produit) as valorisation_totale_machine
    from devices_with_prices
    group by device_id, device_name, device_group
),

-- Résultat final : regroupé par device_name / device_group
final_result as (
    select
        device_name,
        device_group,
        count(device_id) as nombre_machines,
        round(sum(valorisation_totale_machine), 2) as valorisation_totale_machine
    from valorisation_by_device
    group by device_name, device_group
)

-- Ajout métadonnées dbt
select
    device_name,
    device_group,
    nombre_machines,
    valorisation_totale_machine,
    -- Métadonnées d'exécution
    current_timestamp() as dbt_updated_at,
    'cfa737a2-af34-428e-b1db-e5fdf3981e6a' as dbt_invocation_id  -- noqa: CV10, TMP
from final_result
    );
  