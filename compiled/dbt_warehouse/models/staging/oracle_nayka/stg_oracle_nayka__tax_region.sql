

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_tax_region`
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idtax_region as int64) as idtax_region,
        code,
        cast(vat_class as int64) as vat_class,
        cast(idcountry_self as int64) as idcountry_self,
        cast(idcountry_peer as int64) as idcountry_peer,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data