{{ config(materialized='table') }}

-- Valorisation du parc de machines Neshu : copie pour Cockpit Supply de int_oracle_neshu__valorisation_parc_machines
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select * except (dbt_updated_at, dbt_invocation_id)
from {{ ref('int_oracle_neshu__valorisation_parc_machines') }}
