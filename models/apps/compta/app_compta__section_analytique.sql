{{ config(materialized='table') }}

-- Codes que comptapp accepte quand la compta code une dépense à la main : une section en
-- sommeil est inactive dans Sage, elle n'est donc pas proposée.
select
    ca_num as code_analytique,
    trim(ca_intitule) as intitule
from {{ ref('stg_mssql_sage__f_comptea') }}
where ca_sommeil = 0
