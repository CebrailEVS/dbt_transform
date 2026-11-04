
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_resources`
      
    
    

    
    OPTIONS(
      description="""Ressources affect\u00e9es aux t\u00e2ches Nayka (Oracle task_has_resources) : une ligne par t\u00e2che \u00d7 ressource \u00d7 statut d\u2019affectation."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_task_has_resources`
),

cleaned_data as (
    -- Non reprises : quantity, cost, start_km, end_km, isreset (jamais renseignées),
    -- _parent_modification_date (curseur technique de dlt, copie de task.modification_date).
    select
        cast(idtask as int64) as idtask,
        cast(idresources as int64) as idresources,
        cast(task_status as int64) as task_status,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  