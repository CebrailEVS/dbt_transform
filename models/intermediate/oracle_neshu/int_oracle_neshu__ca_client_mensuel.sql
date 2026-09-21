{{
    config(
        materialized='table',
        description='Chiffre d''affaires mensuel par client, ventilé en six composantes'
    )
}}

-- Réunit les six composantes du chiffre d'affaires du P&L client : cinq viennent
-- de la facturation (int_oracle_neshu__facturation_tasks), la sixième de la
-- télémétrie (int_oracle_neshu__ca_telemetrie), qui suit d'autres règles.
--
-- Une ligne de facturation dont le produit ne relève d'aucune des cinq catégories
-- (ca_category à NULL) est IGNORÉE ici, comme dans le rapport Distrilog — environ
-- une ligne sur 850 en août 2026. Elle reste visible dans le modèle amont.

with facturation as (

    select
        date_trunc(date(task_start_date), month) as mois,
        company_id,
        company_code,
        round(sum(if(ca_category = 'VENDING', ca_ht_eur, 0)), 2) as ca_vending_ht_eur,
        round(sum(if(ca_category = 'PRESTA_SERVICE', ca_ht_eur, 0)), 2) as ca_presta_service_ht_eur,
        round(sum(if(ca_category = 'FONTAINES', ca_ht_eur, 0)), 2) as ca_fontaines_ht_eur,
        round(sum(if(ca_category = 'LAVES_VERRES', ca_ht_eur, 0)), 2) as ca_laves_verres_ht_eur,
        round(sum(if(ca_category = 'NEGOCE', ca_ht_eur, 0)), 2) as ca_negoce_ht_eur
    from {{ ref('int_oracle_neshu__facturation_tasks') }}
    where ca_category is not null
    group by mois, company_id, company_code
),

telemetrie as (

    select
        mois,
        company_id,
        company_code,
        ca_nayax_ht_eur
    from {{ ref('int_oracle_neshu__ca_telemetrie') }}
),

-- Un client peut n'avoir que de la facturation, ou que de la télémétrie : le
-- full outer join garde les deux, comme le fait fct_lcdp__ca_mensuel.
assemble as (

    select
        coalesce(f.mois, t.mois) as mois,
        coalesce(f.company_id, t.company_id) as company_id,
        coalesce(f.company_code, t.company_code) as company_code,
        coalesce(f.ca_vending_ht_eur, 0) as ca_vending_ht_eur,
        coalesce(f.ca_presta_service_ht_eur, 0) as ca_presta_service_ht_eur,
        coalesce(f.ca_fontaines_ht_eur, 0) as ca_fontaines_ht_eur,
        coalesce(f.ca_laves_verres_ht_eur, 0) as ca_laves_verres_ht_eur,
        coalesce(f.ca_negoce_ht_eur, 0) as ca_negoce_ht_eur,
        coalesce(t.ca_nayax_ht_eur, 0) as ca_nayax_ht_eur
    from facturation as f
    full outer join telemetrie as t
        on f.mois = t.mois and f.company_id = t.company_id
)

select
    mois,
    company_id,
    company_code,
    ca_vending_ht_eur,
    ca_presta_service_ht_eur,
    ca_fontaines_ht_eur,
    ca_laves_verres_ht_eur,
    ca_negoce_ht_eur,
    ca_nayax_ht_eur,
    round(
        ca_vending_ht_eur + ca_presta_service_ht_eur + ca_fontaines_ht_eur
        + ca_laves_verres_ht_eur + ca_negoce_ht_eur + ca_nayax_ht_eur, 2
    ) as ca_total_ht_eur
from assemble
