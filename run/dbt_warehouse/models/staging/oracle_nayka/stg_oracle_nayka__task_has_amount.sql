
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_amount`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_task_has_amount`
),

cleaned_data as (
    -- _parent_modification_date non reprise : curseur technique de dlt, copie de task.modification_date.
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
  