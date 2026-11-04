
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task`
      
    
    

    
    OPTIONS(
      description="""\u00c9tiquettes pos\u00e9es sur les t\u00e2ches Neshu (Oracle label_has_task) : rattachement t\u00e2che \u2194 \u00e9tiquette."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_label_has_task`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idtask as int64) as idtask,

        -- Timestamps harmonisés
        timestamp(_extracted_at) as extracted_at

    from source_data
),

filtered_data as (
    select lht.*
    from cleaned_data as lht
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
        on lht.idtask = t.idtask
)

select * from filtered_data
    );
  