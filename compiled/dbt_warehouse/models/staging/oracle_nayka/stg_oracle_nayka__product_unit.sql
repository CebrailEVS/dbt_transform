

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_product_unit`
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées, idstring (table string non extraite), idinstance (constante).
    select
        cast(idproduct_unit as int64) as idproduct_unit,
        cast(idproduct as int64) as idproduct,
        cast(idunit_type as int64) as idunit_type,
        code,
        cast(base_unit as int64) as base_unit,
        cast(coeff_multi as float64) as coeff_multi,
        cast(coeff_div as float64) as coeff_div,
        barcode,
        cast(weight as float64) as weight,
        cast(volume as float64) as volume,
        cast(isactive as int64) as isactive,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data