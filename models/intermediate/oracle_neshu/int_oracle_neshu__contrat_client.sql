{{
    config(
        materialized='table',
        description='Date du PREMIER contrat de chaque client, base de la typologie GET / OTHER du P&L'
    )
}}

-- Un client peut porter plusieurs contrats. Le plus ancien date l'entrée en
-- relation, donc la typologie.
--
-- La typologie elle-même (GET = acquis dans l'année) n'est PAS calculée ici : elle
-- dépend de l'année de référence, donc du mois analysé. Elle vit au grain
-- (mois, client), dans le P&L.

with contrats as (

    select
        idcompany_peer as company_id,
        original_start_date,
        updated_at,
        extracted_at
    from {{ ref('stg_oracle_neshu__contract') }}
    where
        idcompany_peer is not null
        and original_start_date is not null
)

select
    company_id,
    min(original_start_date) as first_contract_date,
    count(*) as nb_contrats,
    max(updated_at) as updated_at,
    max(extracted_at) as extracted_at
from contrats
group by company_id
