



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__supply_flux`

where not(stock_total = stock_depot + stock_vehicule)

