



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`

where not(is_out_of_stock_depot = (qty_depot = 0))

