
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__tax_region`
      
    
    

    
    OPTIONS(
      description="""R\u00e9gions fiscales TVA Nayka (Oracle tax_region) : France, Corse, DOM, TOM, Belgique, Luxembourg, avec leurs variantes exon\u00e9r\u00e9es."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_tax_region`
),

cleaned_data as (
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
    );
  