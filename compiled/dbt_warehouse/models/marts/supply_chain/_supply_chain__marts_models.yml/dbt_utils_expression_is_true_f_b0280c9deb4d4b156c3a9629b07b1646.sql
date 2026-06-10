



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_depot_mensuel`

where not(jours_disponibles <= jours_observes)

