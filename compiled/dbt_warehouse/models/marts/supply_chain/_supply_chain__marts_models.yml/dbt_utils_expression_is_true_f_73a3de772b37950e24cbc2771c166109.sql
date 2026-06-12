



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__couverture_stock_neshu`

where not(qte_a_commander >= 0)

