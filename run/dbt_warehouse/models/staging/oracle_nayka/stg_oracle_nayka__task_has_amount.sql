
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_amount`
      
    
    

    
    OPTIONS(
      description="""Montants des t\u00e2ches Nayka par taux de TVA (Oracle task_has_amount) : base HT, TVA et part du total, une ligne par t\u00e2che \u00d7 taux \u00d7 taxe \u00d7 r\u00e9gion fiscale."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_task_has_amount`
),

cleaned_data as (
    select
        cast(idtask as int64) as idtask,
        cast(idtax as int64) as idtax,
        cast(idtax_region as int64) as idtax_region,
        cast(tax_rate as numeric) as tax_rate,
        tax_code,
        tax_name,
        cast(amount_without_tax as numeric) as amount_without_tax,
        cast(tax_amount as numeric) as tax_amount,
        cast(percentage as numeric) as percentage,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  