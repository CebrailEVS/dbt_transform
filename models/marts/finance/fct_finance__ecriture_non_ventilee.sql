{{ config(materialized='view') }}

select
    date_facturation,
    numero_ecriture_comptable as ecriture_comptable_id,
    numero_compte_general,
    libelle_ecriture,
    sens_ecriture,
    date_ecriture_comptable,
    montant,
    extracted_at
from {{ ref('int_mssql_sage__ecriture_non_ventilee') }}
