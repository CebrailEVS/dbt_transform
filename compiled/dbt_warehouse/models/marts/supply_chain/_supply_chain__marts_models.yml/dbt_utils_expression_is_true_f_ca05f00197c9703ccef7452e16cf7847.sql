



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`

where not((rupture_statut is not null) = is_out_of_stock_depot)

