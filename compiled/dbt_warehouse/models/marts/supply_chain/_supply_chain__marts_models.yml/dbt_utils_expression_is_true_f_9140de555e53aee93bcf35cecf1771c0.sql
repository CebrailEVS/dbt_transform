



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__prevision_demande_neshu`

where not(methode_prevision != 'exclu' or demande_prevue_mensuelle = 0)

