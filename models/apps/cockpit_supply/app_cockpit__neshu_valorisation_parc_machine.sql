{{ config(materialized='table') }}

-- Valorisation du parc de machines Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_group,
    device_name,
    nombre_machines,
    valorisation_totale_machine
from {{ ref('int_oracle_neshu__valorisation_parc_machines') }}
