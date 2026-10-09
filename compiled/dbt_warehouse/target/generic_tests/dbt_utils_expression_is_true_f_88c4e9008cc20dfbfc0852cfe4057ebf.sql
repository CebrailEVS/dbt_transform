



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__flux_neshu`

where not(stocks_theoriques_autre >= 0)

