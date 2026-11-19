-- Toute ligne signalée POSE SANS STOCK porte au moins une pièce contrôlée dont la quantité
-- dépasse un stock connu (non null). Retourne les lignes signalées sans pièce qui le justifie.
select d.n_planning
from {{ ref('fct_technique__alerte_conso_aguila_detail') }} as d
where
    d.alerte_compile like '%POSE SANS STOCK%'
    and not exists (
        select 1
        from unnest(d.pieces) as p
        inner join {{ ref('ref_nesp_tech__piece_controle_stock') }} as pc
            on p.code_article = pc.code_article
        where p.stock_van_veille is not null and p.quantite > p.stock_van_veille
    )
