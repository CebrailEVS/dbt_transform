



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__flux_neshu`

where not(stock_total = stock_depot + stock_vehicule)

