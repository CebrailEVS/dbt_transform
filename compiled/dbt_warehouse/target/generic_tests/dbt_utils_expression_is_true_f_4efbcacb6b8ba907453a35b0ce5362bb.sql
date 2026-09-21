



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`

where not(qty_consommee > 0)

