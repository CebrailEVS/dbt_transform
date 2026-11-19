-- Aucun pseudo-article (ligne de saisie d'intervention) ne doit figurer dans le
-- référentiel des pièces ajoutables.
select d.product_reference
from {{ ref('dim_technique__piece_nespresso') }} as d
inner join {{ ref('ref_yuman__pseudo_article') }} as pa
    on lower(d.product_reference) = lower(pa.reference)
