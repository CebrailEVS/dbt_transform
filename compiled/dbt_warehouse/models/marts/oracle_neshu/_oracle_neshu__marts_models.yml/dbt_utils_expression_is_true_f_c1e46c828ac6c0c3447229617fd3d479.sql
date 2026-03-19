



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__supply_flux`

where not(stock_depot >= 0 and stock_vehicule >= 0)

