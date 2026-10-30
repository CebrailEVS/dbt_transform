

with source_checks as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__source_freshness`
),

final as (
    select
        date(finished_at, 'Europe/Paris') as execution_date,
        execution_name as execution_id,
        source_name,
        table_name,
        status,
        timestamp(datetime(max_loaded_at, 'Europe/Paris')) as max_loaded_at,
        timestamp(datetime(checked_at, 'Europe/Paris')) as checked_at,
        age_hours as age_heures
    from source_checks
)

select * from final