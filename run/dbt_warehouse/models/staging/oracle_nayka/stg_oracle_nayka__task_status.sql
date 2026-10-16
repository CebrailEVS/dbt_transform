
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_status`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_task_status`
),

cleaned_data as (
    -- idstring et idstring_action renvoient à la table string, non extraite.
    select
        cast(idtask_status as int64) as idtask_status,
        code,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  