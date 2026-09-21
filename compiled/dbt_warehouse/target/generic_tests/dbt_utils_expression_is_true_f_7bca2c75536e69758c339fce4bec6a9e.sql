



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`

where not(erreur_absolue = abs(erreur))

