
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label_has_task`
      
    
    

    
    OPTIONS(
      description="""\u00c9tiquettes pos\u00e9es sur les t\u00e2ches Nayka (Oracle label_has_task) : une ligne par t\u00e2che \u00d7 \u00e9tiquette."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_label_has_task`
),

cleaned_data as (
    -- _parent_modification_date non reprise : curseur technique de dlt, copie de task.modification_date.
    select
        cast(idtask as int64) as idtask,
        cast(idlabel as int64) as idlabel,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  