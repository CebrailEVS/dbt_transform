
  
    

    create or replace table `evs-datastack-prod`.`prod_export`.`exp_finance__neshu_reception`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9ceptions fournisseurs de Neshu dans Distrilog, au format du fichier \u00ab EVS - NESHU \u00bb envoy\u00e9 chaque mois \u00e0 la direction financi\u00e8re pour rapprocher comptabilit\u00e9 et r\u00e9ceptions.\n[COMMENT CONSTRUITE] T\u00e2ches BL FOURNISSEUR, enregistrement actif, statuts FAIT ou VALIDE, lues dans le staging Oracle Neshu. Colonnes dans l'ordre du fichier ; \u00ab Type t\u00e2che \u00bb est une constante, \u00ab Code DA \u00bb et \u00ab Ressource \u00bb sont vides comme dans l'export Distrilog.\n[GRAIN] 1 ligne par ligne produit d'un bon de r\u00e9ception. Tout l'historique : le job filtre le mois.\n[NOTES] R\u00e9serv\u00e9 au job d'export, pas de rapport Power BI dessus. En-t\u00eates, s\u00e9parateur et format des nombres sont appliqu\u00e9s par le job.\n"""
    )
    as (
      

with receptions as (
    -- 121 = BL FOURNISSEUR ; statuts 1 = FAIT, 4 = VALIDE.
    select
        idtask,
        real_start_date,
        document_number,
        idproduct_source,
        type_product_source,
        idproduct_destination,
        type_product_destination,
        idcompany_peer
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task`
    where
        idtask_type = 121
        and code_status_record = '1'
        and idtask_status in (1, 4)
),

lignes as (
    select
        idtask_has_product,
        idtask,
        idproduct,
        iddevice,
        idproduct_unit,
        name,
        real_quantity,
        unit_coeff_multi,
        sale_amount_net
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product`
),

societes as (
    select
        idcompany,
        code,
        name
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
),

libelles_fr as (
    select
        idstring,
        text
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__string`
    where langage_code = 'fr_FR'
)

select
    r.idtask as task_id,
    date(r.real_start_date, 'Europe/Paris') as reception_date,
    'Bon de réception' as task_type_label,
    origine.code as source_stock_code,
    destination.code as destination_stock_code,
    cast(null as string) as task_device_code,
    machine.code as product_device_code,
    produit.code as product_code,
    produit.name as product_name,
    l.name as task_product_label,
    r.document_number,
    tiers.code as peer_company_code,
    tiers.name as peer_company_name,
    cast(null as string) as resource_name,
    cast(l.real_quantity as numeric) as quantity,
    unite.text as unit_label,
    cast(l.unit_coeff_multi as numeric) as unit_coefficient,
    cast(l.sale_amount_net as numeric) as amount_excl_tax,
    fournisseur.name as main_supplier_name,
    l.idtask_has_product as task_product_id
from receptions as r
inner join lignes as l on r.idtask = l.idtask
left join societes as origine
    on r.idproduct_source = origine.idcompany and r.type_product_source = 'COMPANY'
left join societes as destination
    on r.idproduct_destination = destination.idcompany and r.type_product_destination = 'COMPANY'
left join societes as tiers on r.idcompany_peer = tiers.idcompany
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as machine on l.iddevice = machine.iddevice
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as produit on l.idproduct = produit.idproduct
left join societes as fournisseur on produit.idcompany_supplier = fournisseur.idcompany
left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product_unit` as conditionnement
    on l.idproduct_unit = conditionnement.idproduct_unit
left join libelles_fr as unite on conditionnement.idstring = unite.idstring
    );
  