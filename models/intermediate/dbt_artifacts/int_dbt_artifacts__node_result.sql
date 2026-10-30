{{ config(materialized='view') }}

with executions as (
    select * from {{ ref('stg_dbt_artifacts__runner_execution') }}
),

node_results as (
    select
        e.execution_name,
        e.selection,
        e.finished_at,
        node.unique_id,
        split(node.unique_id, '.')[safe_offset(0)] as resource_type,
        -- unit_test.<projet>.<modèle>.<test> : le nom du test est au 4e rang, pas au 3e.
        split(node.unique_id, '.')[safe_offset(if(node.unique_id like 'unit_test.%', 3, 2))] as node_name,
        node.status,
        node.failures,
        node.message,
        node.execution_time,
        node.relation_name,
        node.bigquery_job_id
    from executions as e
    cross join unnest(e.node_results) as node
)

select * from node_results
