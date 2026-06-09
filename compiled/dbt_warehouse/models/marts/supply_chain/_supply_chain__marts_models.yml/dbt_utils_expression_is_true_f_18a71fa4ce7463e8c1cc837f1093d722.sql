



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_mensuel`

where not(taux_disponibilite_pct between 0 and 100)

