{{ config(materialized='table') }}

select * from {{ ref('int_nesp_co__opportunites') }}
