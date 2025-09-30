

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_task_has_product`
),

cleaned_data as (
    select
        -- IDs (clés primaires et étrangères)
        cast(idtask_has_product as int64) as idtask_has_product,
        cast(idtask as int64) as idtask,
        cast(idproduct as int64) as idproduct,
        cast(idproduct_source as int64) as idproduct_source,
        cast(idproduct_destination as int64) as idproduct_destination,
        cast(idtax as int64) as idtax,
        cast(idtask_has_product_associed as int64) as idtask_has_product_associed,
        cast(iddevice as int64) as iddevice,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idlocation as int64) as idlocation,

        -- Colonnes texte et types
        type_product_source,
        type_product_destination,
        code,
        name,
        cast(priceline_number as int64) as priceline_number,

        -- Colonnes numériques
        cast(real_quantity as float64) as real_quantity,
        cast(average_purchase_price as float64) as average_purchase_price,
        cast(tax_rate as float64) as tax_rate,
        cast(tax_amount as float64) as tax_amount,
        cast(global_discount_amount as float64) as global_discount_amount,
        cast(net_price as float64) as net_price,
        cast(sale_amount_net_tax as float64) as sale_amount_net_tax,
        cast(sale_amount_net as float64) as sale_amount_net,
        cast(sale_unit_price as float64) as sale_unit_price,
        cast(purchase_unit_price as float64) as purchase_unit_price,
        cast(sale_amount_brut as float64) as sale_amount_brut,
        cast(unit_coeff_multi as float64) as unit_coeff_multi,
        cast(sale_unit_price_tax as float64) as sale_unit_price_tax,
        cast(unit_coeff_div as float64) as unit_coeff_div,

        -- Timestamps harmonisés (standard DBT)
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
),

-- ⚠️ Sécurité de jointure pour éviter les orphelins
filtered_data as (
    select c.*
    from cleaned_data c
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` t
        on c.idtask = t.idtask
)

select * from filtered_data

    where updated_at >= (
        select max(updated_at) - interval 1 day
        from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product`
    )
