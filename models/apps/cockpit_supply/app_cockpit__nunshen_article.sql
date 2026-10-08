{{ config(materialized='table') }}

-- Articles Nunshen (Sage) pour Cockpit Supply : remplace l'import « Base articles ».
-- Colonnes nommées comme la table d'import de l'app. Champs libres Sage OUI/NON → booléens.
select
    ar.ar_ref as reference,
    ar.ar_design as designation,
    ar.ar_sommeil = 0 as actif,
    ar.fa_code_famille as code_famille,
    fa.fa_intitule as libelle_famille,
    coalesce(ar.signature_exclusivite_client = 'OUI', false) as signature_exclusivite_client,
    coalesce(ar.exclusivite_boutique = 'OUI', false) as exclusivite_boutique,
    coalesce(ar.produit_sur_demande = 'OUI', false) as produit_sur_demande,
    ar.extracted_at
from {{ ref('stg_mssql_sage__f_article') }} as ar
left join {{ ref('stg_mssql_sage__f_famille') }} as fa
    on ar.fa_code_famille = fa.fa_code_famille
