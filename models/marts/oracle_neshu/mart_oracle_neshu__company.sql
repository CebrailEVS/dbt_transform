{{
    config(
        materialized='table',
        cluster_by=['idcompany'],
        description='Résumé des entreprises par type avec métriques agrégées'
    )
}}

with company_data as (
    select *
    from {{ ref('stg_oracle_neshu__company') }}  -- Remplacez par le nom exact de votre modèle staging
)

select * from company_data