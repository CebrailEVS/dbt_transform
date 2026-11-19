-- Une pièce n'apparaît qu'une fois par intervention : l'application retraite une pièce
-- par (n_planning, code_article), un doublon compterait deux fois la même pose.
select
    d.n_planning,
    p.code_article,
    count(*) as nb
from {{ ref('fct_technique__alerte_conso_aguila_detail') }} as d
cross join unnest(d.pieces) as p
group by d.n_planning, p.code_article
having count(*) > 1
