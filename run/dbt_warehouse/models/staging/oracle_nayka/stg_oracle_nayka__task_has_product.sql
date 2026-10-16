
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_product`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_task_has_product`
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées, iduser_creation / iduser_modification (table user
    -- non extraite ; idcontact_* portent l'auteur), discount_input (constante au sens non documenté).
    select
        cast(idtask_has_product as int64) as idtask_has_product,
        cast(idtask as int64) as idtask,
        cast(idproduct as int64) as idproduct,
        cast(idproduct_unit as int64) as idproduct_unit,
        cast(iddevice as int64) as iddevice,
        cast(idcontract as int64) as idcontract,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idlocation as int64) as idlocation,
        cast(idtax as int64) as idtax,
        cast(idtax_region as int64) as idtax_region,
        cast(idtask_has_product_associed as int64) as idtask_has_product_associed,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        -- Source et destination de la ligne : une société ou un équipement, selon type_product_*.
        cast(idproduct_source as int64) as idproduct_source,
        type_product_source,
        cast(idproduct_destination as int64) as idproduct_destination,
        type_product_destination,

        code,
        name,
        real_planned,
        cast(sequence as int64) as sequence,  -- noqa: RF04

        cast(real_quantity as float64) as real_quantity,
        unit_code,
        unit_text,
        cast(unit_coeff_multi as float64) as unit_coeff_multi,
        cast(unit_coeff_div as float64) as unit_coeff_div,
        cast(unit_weight as float64) as unit_weight,
        cast(unit_volume as float64) as unit_volume,

        cast(tax_rate as numeric) as tax_rate,
        tax_code,
        tax_name,

        cast(sale_unit_price as numeric) as sale_unit_price,
        cast(sale_unit_price_tax as numeric) as sale_unit_price_tax,
        cast(net_price as numeric) as net_price,
        cast(packaging_price as numeric) as packaging_price,
        cast(purchase_unit_price as numeric) as purchase_unit_price,
        cast(average_purchase_price as numeric) as average_purchase_price,
        cast(sale_amount_brut as numeric) as sale_amount_brut,
        cast(sale_amount_net as numeric) as sale_amount_net,
        cast(sale_amount_net_tax as numeric) as sale_amount_net_tax,
        cast(tax_amount as numeric) as tax_amount,
        cast(discount_rate as numeric) as discount_rate,
        cast(amount_price_increase as numeric) as amount_price_increase,
        cast(amount_price_reduction as numeric) as amount_price_reduction,

        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  