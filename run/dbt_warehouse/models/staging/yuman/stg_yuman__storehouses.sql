-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses` as DBT_INTERNAL_DEST
        using (

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_storehouses`

),

cleaned_storehouses as (

    select
        id as storehouses_id,
        name as storehouses_name,
        address as storehouses_address,
        timestamp(_sdc_extracted_at) as extracted_at,
        cast(null as timestamp) as deleted_at
    from source_data

)

select *
from cleaned_storehouses



    union all

    -- lignes supprimées depuis la source
    select
        s.storehouses_id,
        s.storehouses_name,
        s.storehouses_address,
        s.extracted_at,
        current_timestamp() as deleted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses` as s
    left join cleaned_storehouses as c
        on s.storehouses_id = c.storehouses_id
    where c.storehouses_id is null


        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.storehouses_id = DBT_INTERNAL_DEST.storehouses_id))

    
    when matched then update set
        `storehouses_id` = DBT_INTERNAL_SOURCE.`storehouses_id`,`storehouses_name` = DBT_INTERNAL_SOURCE.`storehouses_name`,`storehouses_address` = DBT_INTERNAL_SOURCE.`storehouses_address`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`,`deleted_at` = DBT_INTERNAL_SOURCE.`deleted_at`
    

    when not matched then insert
        (`storehouses_id`, `storehouses_name`, `storehouses_address`, `extracted_at`, `deleted_at`)
    values
        (`storehouses_id`, `storehouses_name`, `storehouses_address`, `extracted_at`, `deleted_at`)


    