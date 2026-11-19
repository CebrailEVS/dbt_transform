-- POSE SANS STOCK se lit dans les deux sens : toute ligne signalée porte une pièce contrôlée dont
-- la quantité dépasse un stock connu (non null), et toute ligne qui en porte une est signalée.
-- Retourne une ligne par écart.
with pose_sans_stock as (
    select
        d.n_planning,
        d.alerte_compile like '%POSE SANS STOCK%' as est_signalee,
        exists(
            select 1
            from unnest(d.pieces) as p
            inner join {{ ref('ref_nesp_tech__piece_controle_stock') }} as pc
                on p.code_article = pc.code_article
            where p.stock_van_veille is not null and p.quantite > p.stock_van_veille
        ) as a_une_pose_sans_stock
    from {{ ref('fct_technique__alerte_conso_aguila_detail') }} as d
)

select
    n_planning,
    est_signalee,
    a_une_pose_sans_stock
from pose_sans_stock
where est_signalee != a_une_pose_sans_stock
