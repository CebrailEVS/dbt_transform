

 with max_recency as (

    select max(cast(extracted_at as timestamp)) as max_timestamp
    from
        `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu_gcs__stock_theorique`
    where
        -- to exclude erroneous future dates
        cast(extracted_at as timestamp) <= timestamp(datetime(current_timestamp(), 'Europe/Paris'))
        
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
        interval -14 day
        )

 as timestamp)



