



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`

where not(demande_retenue_journaliere >= demande_prevue_journaliere - 0.001)

