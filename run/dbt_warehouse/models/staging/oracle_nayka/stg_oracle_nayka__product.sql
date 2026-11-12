
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product`
      
    
    

    
    OPTIONS(
      description="""Produits Nayka (Oracle product) : produits vendus, pi\u00e8ces, mod\u00e8les de machines (cible de device.idmodel), moyens de paiement et produits calcul\u00e9s. code n'est unique qu'au sein d'un type de produit."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_product`
),

cleaned_data as (
    select
        cast(idproduct as int64) as idproduct,
        cast(idproduct_type as int64) as idproduct_type,
        cast(idcompany_supplier as int64) as idcompany_supplier,
        cast(idtax as int64) as idtax,
        cast(idtax_purchase as int64) as idtax_purchase,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        code,
        name,
        commercial_name,
        external_reference,
        comments,
        cast(code_status_record as int64) as code_status_record,
        cast(system as int64) as system,  -- noqa: RF04

        cast(tariff_without_tax as numeric) as tariff_without_tax,
        cast(tariff_with_tax as numeric) as tariff_with_tax,
        cast(purchase_unit_price as numeric) as purchase_unit_price,
        cast(average_purchase_price as numeric) as average_purchase_price,

        cast(stock_management as int64) as stock_management,
        cast(prohibited_purchase as int64) as prohibited_purchase,
        cast(prohibited_sale as int64) as prohibited_sale,
        cast(reconditionnable as int64) as reconditionnable,
        cast(unconsign as int64) as unconsign,
        cast(traceability_required as int64) as traceability_required,
        cast(made_up as int64) as made_up,
        cast(unsold as int64) as unsold,
        cast(weight as float64) as weight,

        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  