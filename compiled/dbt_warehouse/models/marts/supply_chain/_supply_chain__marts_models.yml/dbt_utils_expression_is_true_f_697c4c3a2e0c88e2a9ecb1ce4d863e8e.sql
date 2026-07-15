



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`

where not(methode_prevision != 'exclu' or prevision_retenue = 0)

