
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

 with max_recency as (

    select max(cast(date_intervention as timestamp)) as max_timestamp
    from
        `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles`
    where
        -- to exclude erroneous future dates
        cast(date_intervention as timestamp) <= timestamp(datetime(current_timestamp(), 'Europe/Paris'))
        
)
select
    *
from
    max_recency
where
    -- if the row_condition excludes all rows, we need to compare against a default date
    -- to avoid false negatives
    coalesce(max_timestamp, cast('1970-01-01' as timestamp))
        <
        cast(

        datetime_add(
            cast( timestamp(datetime(current_timestamp(), 'Europe/Paris')) as datetime),
        interval -8 day
        )

 as timestamp)




  
  
      
    ) dbt_internal_test