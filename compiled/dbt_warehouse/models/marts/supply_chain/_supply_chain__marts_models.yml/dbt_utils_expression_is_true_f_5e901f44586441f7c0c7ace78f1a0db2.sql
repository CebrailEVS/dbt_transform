



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__couverture_stock_neshu`

where not(statut != 'RUPTURE TOTALE' or (conso_journaliere_n1 > 0 and stock_actuel <= 0))

