{{ config(
    materialized='table'
) }}

with pseudo_articles as (
    select reference from {{ ref('ref_yuman__pseudo_article') }}
),

pieces as (
    select
        p.product_code as product_reference,
        p.product_name as libelle,
        upper(substr(p.product_code, length('EVS_NESPRESSO_') + 1)) as code_article,
        p.product_id
    from {{ ref('stg_yuman__products') }} as p
    left join pseudo_articles as pa
        on p.product_code = pa.reference
    where
        starts_with(p.product_code, 'EVS_NESPRESSO_')
        and p.is_active
        and not coalesce(p.is_forbidden_article, false)
        and pa.reference is null
),

-- Yuman distingue la casse : deux fiches peuvent porter le même code ne différant que par
-- la casse. On garde la référence en majuscules (celle des marts de consommation Nespresso),
-- à défaut la plus ancienne.
pieces_dedup as (
    select
        code_article,
        product_reference,
        libelle
    from pieces
    qualify row_number() over (
        partition by code_article
        order by (product_reference = 'EVS_NESPRESSO_' || code_article) desc, product_id
    ) = 1
)

select
    pieces_dedup.code_article,
    pieces_dedup.product_reference,
    pieces_dedup.libelle,
    -- Un prix à 0 du seed (« Non Référencé CORDON ») est un tarif inconnu, pas une pièce gratuite.
    nullif(prix.article_prix_unitaire, 0) as prix_unitaire
from pieces_dedup
-- Même jointure que fct_technique__piece_detachee_pricing_nespresso.
left join {{ ref('ref_nesp_tech__articles_prix') }} as prix
    on lower(pieces_dedup.code_article) = lower(prix.article_ref_nomad)
