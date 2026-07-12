



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`

where not((methode_prevision = 'exclu') = (statut_vie in ('arrete', 'inactif')))

